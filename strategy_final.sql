CREATE TYPE think_states 
AS ENUM('CREATE_OFFERS',
        'SHIP_LOAD', 
        'MOVE_TO_VENDOR_ISLAND', 
        'MOVE_TO_CUSTOMER_ISLAND', 
        'SHIP_UNLOAD');
CREATE TABLE states(
    id SERIAL PRIMARY KEY NOT NULL,
    think_state THINK_STATES NOT NULL,
    ship INTEGER NOT NULL,
    item INTEGER,
    island_c INTEGER,
    island_v INTEGER,
    customer INTEGER,
    vendor INTEGER,
    quantity_offer DOUBLE PRECISION,
    offer_v INTEGER,
    offer_c INTEGER
);

CREATE PROCEDURE Initialize(player_id INTEGER) AS $$
DECLARE new_ship RECORD;
BEGIN
  
   INSERT INTO public.states(think_state, ship, item, island_c, island_v, customer, vendor, quantity_offer, offer_v, offer_c)
   SELECT 
   'CREATE_OFFERS', id, 0, 0, 0, 0, 0, 0, 0, 0
   FROM world.ships
   WHERE player = player_id;
   
END
$$ LANGUAGE PLPGSQL;

CREATE PROCEDURE CREATE_OFFERS(ship_id INTEGER) AS $$
DECLARE 
offers RECORD;
offer_customer INTEGER;
offer_vendor INTEGER;
begin
WITH 
parked_ships AS (
                SELECT 
                    ships.id as ship,
                    island as island_p,
                    capacity,
                    speed
                FROM world.ships
                JOIN world.parked_ships
                ON ships.id=ship_id
                AND ships.id = parked_ships.ship
                JOIN public.states
                ON ships.id = states.ship
                AND states.think_state = 'CREATE_OFFERS'),
vendors AS (
            SELECT 
                contractors.id as vendor, 
                contractors.island as island_v, 
                contractors.item as item_v, 
                contractors.quantity as quantity_v,
                contractors.price_per_unit as price_v
            FROM world.contractors
            WHERE contractors."type" = 'vendor'
            AND NOT contractors.id IN (SELECT vendor FROM public.states WHERE ship <> ship_id)),
customers AS (
            SELECT
                contractors.id as customer, 
                contractors.island as island_c, 
                contractors.item as item_c, 
                contractors.quantity as quantity_c,
                contractors.price_per_unit as price_c
            FROM world.contractors
            WHERE contractors."type" = 'customer'
            AND NOT contractors.id IN (SELECT customer FROM public.states WHERE ship <> ship_id)),
offers AS (
            SELECT 
                    vendor, 
                    customer, 
                    island_v, 
                    island_c,
                    item_v as item_offer,
                    LEAST(quantity_v, quantity_c) as quantity_offer,
                    price_c,
                    price_v
            FROM vendors
            JOIN customers
            ON item_v = item_c),
ships_offers AS (
            SELECT 
                vendor, 
                customer,
                island_p,
                island_v, 
                island_c,
                item_offer,
                LEAST(quantity_offer, capacity) as order_quantity,
                LEAST(quantity_offer, capacity) * (price_c - price_v) as profit,
                speed
            FROM  
            offers, 
            parked_ships),
top_price AS (
        SELECT
    vendor,
    customer,
    island_v,
    island_c,
    item_offer,
    order_quantity,
    profit / ( ( (
                SQRT(
                    POWER(
                        vendor_island.x - parked_island.x,
                        2
                    ) + POWER(
                        vendor_island.y - parked_island.y,
                        2
                    )
                ) + SQRT(
                    POWER(
                        customer_island.x - vendor_island.x,
                        2
                    ) + POWER(
                        customer_island.y - vendor_island.y,
                        2
                    )
                )
            ) / speed + (order_quantity * 2)
        )
    ) AS profit
FROM ships_offers
    JOIN world.islands AS vendor_island ON ships_offers.island_v = vendor_island.id
    JOIN world.islands AS parked_island ON ships_offers.island_p = parked_island.id
    JOIN world.islands AS customer_island ON ships_offers.island_c = customer_island.id
ORDER BY profit DESC
LIMIT 1)
SELECT
    vendor, 
    customer, 
    island_v, 
    island_c,
    item_offer, 
    order_quantity INTO offers
FROM top_price;

    IF NOT offers.customer IS NULL THEN 
        INSERT INTO actions.offers(contractor, quantity) VALUES (offers.customer, offers.order_quantity) RETURNING id INTO offer_customer;
        INSERT INTO actions.offers(contractor, quantity) VALUES (offers.vendor, offers.order_quantity) RETURNING id INTO offer_vendor;
       
        UPDATE public.states
        SET (think_state, island_c, island_v, customer, vendor, item, quantity_offer, offer_v, offer_c)
        = ('MOVE_TO_VENDOR_ISLAND', offers.island_c, offers.island_v, offers.customer, offers.vendor, offers.item_offer, offers.order_quantity, offer_vendor, offer_customer)
        WHERE ship = ship_id;
        --RAISE NOTICE 'SHIP:% VENDOR:% CUSTOMER:%', ship_id, offers.vendor, offers.customer;
    ELSE 
        RAISE NOTICE 'WAITING';
        CALL WAIT(1);
    END IF;

end 
$$ LANGUAGE PLPGSQL;

CREATE procedure SHIP_LOAD(player_id INTEGER, ship_id INTEGER) as $$
DECLARE loading_state RECORD;
begin
        SELECT
            states.ship,
            states.item,
            quantity_offer as quantity,
            'load' as direction,
            CASE 
                WHEN quantity_offer = COALESCE(cargo.quantity, 0) 
            THEN TRUE 
            ELSE FALSE 
            END as want_move, 
            CASE 
                WHEN quantity_offer <= COALESCE(storage.quantity, 0) 
            THEN TRUE 
            ELSE FALSE 
            END as want_load INTO loading_state           
        FROM public.states
        LEFT JOIN world.cargo
        ON states.ship = cargo.ship
        AND states.item = cargo.item
        LEFT JOIN world.storage
        ON states.island_v = storage.island
        AND states.item = storage.item
        AND states.quantity_offer <= storage.quantity
        AND storage.player = player_id
        WHERE think_state = 'SHIP_LOAD'
        AND states.ship = ship_id;
        
        IF loading_state.want_move OR EXISTS(SELECT ship FROM events.transfer_completed WHERE ship = ship_id) THEN  
            UPDATE public.states
            SET think_state = 'MOVE_TO_CUSTOMER_ISLAND'
            WHERE ship = ship_id;

            CALL MOVE_TO_CUSTOMER_ISLAND(player_id, ship_id);
        ELSEIF loading_state.want_load THEN
            INSERT INTO actions.transfers(ship, item, quantity, direction)
            VALUES (loading_state.ship, loading_state.item, loading_state.quantity, 'load');
        ELSE
            CALL WAIT(1);
        END IF;
    
end
$$ language plpgsql;

CREATE procedure SHIP_UNLOAD(player_id INTEGER, ship_id INTEGER) as $$
DECLARE unloading_state RECORD;
begin

        SELECT
            states.ship,
            states.item,
            quantity_offer as quantity,
            'unload' as direction,     
            CASE 
                WHEN COALESCE(cargo.quantity, 0) = 0 
            THEN TRUE
            ELSE FALSE
            END as is_empty INTO unloading_state     
        FROM public.states
        LEFT JOIN world.cargo
        ON states.ship = cargo.ship
        WHERE states.ship = ship_id
        AND states.think_state = 'SHIP_UNLOAD';
        
        IF unloading_state.is_empty OR EXISTS(SELECT ship FROM events.transfer_completed WHERE ship = ship_id) THEN
            UPDATE public.states
            SET think_state = 'CREATE_OFFERS'
            WHERE ship = ship_id; 

            CALL CREATE_OFFERS(ship_id);
        ELSEIF NOT unloading_state.is_empty THEN      
            INSERT INTO actions.transfers(ship, item, quantity, direction)
            VALUES (unloading_state.ship, unloading_state.item, unloading_state.quantity, 'unload');
        ELSE
            CALL WAIT(1);
        END IF;
    
end
$$ language plpgsql;

CREATE procedure MOVE_TO_VENDOR_ISLAND(player_id INTEGER, ship_id INTEGER) as $$
DECLARE moving_state RECORD;
begin
    SELECT
        states.ship,
        island_c,
        island_v,
        item,
        customer,
        vendor,
        COALESCE(parked_ships.island, 0) as on_island_v,
        COALESCE(contracts.id, 0) as have_contract,
        vendor_check.offer IS NULL AND customer_check.offer IS NULL as offer_confirm,
        0 as move_finished
        INTO moving_state
    FROM public.states
    LEFT JOIN world.contracts
    ON states.customer = contracts.contractor
    AND contracts.player = player_id
    LEFT JOIN events.offer_rejected as vendor_check
    ON states.offer_v = vendor_check.offer
    LEFT JOIN events.offer_rejected as customer_check
    ON states.offer_c = customer_check.offer 
    LEFT JOIN world.parked_ships
    ON states.ship = parked_ships.ship
    AND states.island_v = parked_ships.island

    WHERE think_state = 'MOVE_TO_VENDOR_ISLAND'
    AND states.ship = ship_id;
    
    IF moving_state.have_contract = 0 OR NOT moving_state.offer_confirm THEN
              
                UPDATE public.states
                SET (think_state, item, island_c, island_v, customer, vendor, offer_v, offer_c)
                = ('CREATE_OFFERS', 0, 0, 0, 0, 0, 0, 0)
                WHERE ship = ship_id;

                CALL CREATE_OFFERS(ship_id);
    
       -- RAISE NOTICE 'SHIP % NEED NEW OFFER', ship_id;
    ELSEIF moving_state.on_island_v > 0 OR EXISTS(SELECT ship FROM events.ship_move_finished WHERE ship = ship_id) THEN

        UPDATE public.states
        SET think_state = 'SHIP_LOAD'
        WHERE ship = ship_id;
        
        CALL SHIP_LOAD(player_id, ship_id);
  
    ELSEIF moving_state.island_c > 0 AND moving_state.island_v > 0 THEN
        INSERT INTO actions.ship_moves(ship, destination) VALUES (ship_id, moving_state.island_v);
    ELSE 
        CALL WAIT(1);
    END IF;
end
$$ language plpgsql;

CREATE procedure MOVE_TO_CUSTOMER_ISLAND(player_id INTEGER, ship_id INTEGER) as $$
DECLARE moving_state_c RECORD;
begin
    SELECT
        states.ship,
        island_c,
        island_v,
        item,
        customer,
        vendor,
        COALESCE(parked_ships.island, 0) as on_island_c
    INTO moving_state_c
    FROM public.states
    LEFT JOIN world.parked_ships
    ON states.ship = parked_ships.ship
    AND states.island_c = parked_ships.island
    WHERE think_state = 'MOVE_TO_CUSTOMER_ISLAND'
    AND states.ship = ship_id;

    IF moving_state_c.on_island_c > 0 OR EXISTS(SELECT ship FROM events.ship_move_finished WHERE ship = ship_id) THEN
        UPDATE public.states
        SET think_state = 'SHIP_UNLOAD'
        WHERE ship = ship_id;

        CALL SHIP_UNLOAD(player_id, ship_id);
        
    ELSEIF moving_state_c.island_c > 0 THEN
        INSERT INTO actions.ship_moves(ship, destination) VALUES (ship_id, moving_state_c.island_c);
    ELSE 
        CALL WAIT(1);
    END IF;
end
$$ language plpgsql;

CREATE procedure WAIT(until_time DOUBLE PRECISION) as $$
begin
    INSERT INTO actions.wait(until) VALUES (until_time);
end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER) LANGUAGE PLPGSQL AS $$
declare
    currentTime double precision;
    moneyTime DOUBLE PRECISION;
    ship_state record;
BEGIN
    SELECT game_time INTO currentTime FROM world.global;
    select money into moneyTime from world.players where id=player_id;
    IF currentTime = 0 THEN CALL INITIALIZE(player_id); END IF;
    RAISE NOTICE 'time % money %', currentTime, moneyTime;
    FOR ship_state IN SELECT states.ship, states.think_state FROM public.states JOIN world.ships ON states.ship = ships.id LOOP
            
            CASE ship_state.think_state
                WHEN 'CREATE_OFFERS' THEN
                    CALL CREATE_OFFERS(ship_state.ship);  
                WHEN 'MOVE_TO_VENDOR_ISLAND' THEN
                    IF NOT EXISTS (SELECT moving_ships.ship FROM world.moving_ships WHERE moving_ships.ship = ship_state.ship) THEN
                        CALL MOVE_TO_VENDOR_ISLAND(player_id, ship_state.ship);
                    ELSE
                        CALL WAIT(1);
                    END IF;
                WHEN 'SHIP_LOAD' THEN
                    IF NOT EXISTS (SELECT transferring_ships.ship FROM world.transferring_ships WHERE transferring_ships.ship = ship_state.ship) THEN
                        CALL SHIP_LOAD(player_id, ship_state.ship);
                    ELSE
                        CALL WAIT(1);
                    END IF;
                WHEN 'MOVE_TO_CUSTOMER_ISLAND' THEN
                    IF NOT EXISTS (SELECT moving_ships.ship FROM world.moving_ships WHERE moving_ships.ship = ship_state.ship) THEN
                        CALL MOVE_TO_CUSTOMER_ISLAND(player_id, ship_state.ship);
                    ELSE
                        CALL WAIT(1);
                    END IF;
                WHEN 'SHIP_UNLOAD' THEN
                    IF NOT EXISTS (SELECT moving_ships.ship FROM world.moving_ships WHERE moving_ships.ship = ship_state.ship)
                    AND NOT EXISTS (SELECT transferring_ships.ship FROM world.transferring_ships WHERE transferring_ships.ship = ship_state.ship) THEN
                        CALL SHIP_UNLOAD(player_id, ship_state.ship);
                    ELSE
                        CALL WAIT(1);
                    END IF;
                ELSE
                    CALL WAIT(1);
            END CASE;
        END LOOP;

END $$;