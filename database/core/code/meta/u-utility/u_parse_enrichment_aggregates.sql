-- parse all aggregates in the expression into temp table
CREATE OR REPLACE FUNCTION meta.u_parse_enrichment_aggregates(v_expression text)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_expression_position        INT     := 0;
    v_in_quotes_flag             BOOLEAN := FALSE;
    v_parentheses_depth          INT     := 0;
    v_expression_length          INT;
    v_aggregates_exist_flag      BOOLEAN;
    v_aggregate_name             TEXT;
    v_aggregate_start_position   INT;
    v_char                       CHAR;
    v_prev_char                  CHAR;
    v_next_char                  CHAR;
    v_window_function_flag       BOOLEAN;
    v_in_aggregate_flag          BOOLEAN;
    v_follow_text                TEXT;
    v_inner_text                 TEXT;
    v_inner_text_quoted_removed  TEXT;
    v_aggregates                 TEXT;

BEGIN

SELECT string_agg(aggregate_name,'|') INTO v_aggregates
FROM meta.aggregate;

DROP TABLE IF EXISTS _aggs_parsed;
CREATE TEMP TABLE _aggs_parsed 
    (id int,
    function text,
    a_function_start int,
    a_start int,
    a_end int,
    expression text,
    expression_parsed text,
    relation_ids int[]
    ) ON COMMIT DROP;

    v_expression_length = length(v_expression);

    WHILE v_expression_position < v_expression_length LOOP
        v_expression_position := v_expression_position + 1;
        v_prev_char = CASE WHEN v_expression_position > 1 THEN substring(v_expression, v_expression_position - 1, 1) ELSE ' ' END;
        v_char = substring(v_expression, v_expression_position, 1);
        IF v_in_aggregate_flag AND NOT v_in_quotes_flag THEN
            --Update parenthesis
            IF v_char = ')' THEN
                v_parentheses_depth := v_parentheses_depth - 1;
            ELSEIF v_char = '(' THEN
                v_parentheses_depth := v_parentheses_depth + 1;
            END IF;

            IF v_parentheses_depth = 0
            THEN
                -- Check for window function
                v_follow_text := substring(v_expression, v_expression_position + 1, length(v_expression));
                v_window_function_flag := v_follow_text ~* '^\s*over\s*\(';

                RAISE DEBUG 'Out of parenthesis. position=% v_window_function_flag=% v_follow_text=%',v_expression_position,v_window_function_flag, v_follow_text;
                v_inner_text := substring(v_expression, v_aggregate_start_position, v_expression_position - v_aggregate_start_position);
                v_inner_text_quoted_removed := regexp_replace(v_inner_text, '(''[^'']+'')', '','g');

                -- Check for nested aggregates or window functions
                v_aggregate_name := lower((regexp_match(v_inner_text_quoted_removed,format('(?:^|[^\w])(%s)\(', v_aggregates),'i'))[1]);

                IF v_aggregate_name IS NOT NULL THEN
                   RETURN format('Nested aggregate or window function `%s` found inside inner aggregate expression starting at position %s. Please break it up into separate rule.', v_aggregate_name, v_aggregate_start_position);
                END IF; 

                IF v_window_function_flag THEN
                    IF v_aggregates_exist_flag THEN
                        RETURN 'Cannot have a window function and an aggregate in the same rule. Please break them into separate rules and combine later.Position: ' || v_expression_position;
                    END IF;
                    DELETE FROM _aggs_parsed WHERE a_start = v_aggregate_start_position;
                ELSE
                    v_aggregates_exist_flag := true;
                    --We've got the whole aggregate, update end position
                    UPDATE _aggs_parsed SET a_end = v_expression_position, 
                        expression = v_inner_text
                    WHERE a_start = v_aggregate_start_position;
                END IF;
                v_in_aggregate_flag := FALSE;
            ELSE
                --Still in an aggregate. Keep going.
            END IF;
        
        ELSEIF v_char = '(' AND NOT v_in_quotes_flag THEN 
                --check if we're at the start of aggregate

                v_aggregate_name := lower((regexp_match(substring(v_expression,1, v_expression_position),format('(?:^|[^\w])(%s)\($', v_aggregates),'i'))[1]);

                IF v_aggregate_name IS NOT NULL THEN
                    --we are at the start of aggregate function

                    RAISE DEBUG 'Found aggregate % at position=%',v_aggregate_name, v_expression_position;
                    v_aggregate_start_position := v_expression_position + 1;
            
                    -- save it and move cursor to the beginning of (arguments)
                    INSERT INTO _aggs_parsed(a_start, a_function_start, function) VALUES (v_aggregate_start_position, v_expression_position - length(v_aggregate_name), v_aggregate_name);
                    -- v_expression_position := v_aggregate_start_position - 1;
                    v_parentheses_depth = 1;
                    v_in_aggregate_flag := true;
                END IF;
        END IF;

        IF (v_in_quotes_flag)
        THEN
            IF v_char = ''''
            THEN
                v_next_char := substring(v_expression, v_expression_position + 1, 1);
                IF v_next_char = ''''
                THEN
                    --Double quote escape character. Keep going
                    v_expression_position := v_expression_position + 1;
                ELSE
                    v_in_quotes_flag := FALSE;
                END IF;
            ELSE
                --Still in a quoted string, keep going down the string
            END IF;

        ELSEIF v_char = '''' THEN
                v_in_quotes_flag = TRUE;
        END IF;

    END LOOP;

    IF v_in_aggregate_flag THEN
        RETURN 'Unclosed parenthesis for aggregate starting at position ' || v_aggregate_start_position;
    END IF;


WITH pos AS (SELECT a_start, ROW_NUMBER() OVER(ORDER BY a_start) id FROM _aggs_parsed)
UPDATE _aggs_parsed a SET id = pos.id
FROM pos
WHERE a.a_start = pos.a_start;

UPDATE _params SET aggregation_id = null WHERE aggregation_id NOT IN (SELECT id FROM _aggs_parsed);

RETURN '';

END;

$BODY$;