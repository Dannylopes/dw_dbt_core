WITH
origem AS (
        SELECT
              "Date"
            , "Close"
            , "simbolo"
        FROM {{source('atomdb', 'tb_commodities')}}
),

destino AS (
        SELECT
              CAST("Date" as date)  AS Data
            , "Close"               AS valor_fechamento
            , "simbolo"             AS ticker_ativo
        FROM origem
)

SELECT * FROM destino