SELECT a.contract_id, zipcode, tariff_code,
 power_kw, client_type_description FROM (SELECT
  contract_id
, supply_point_code
, is_sale_flag
, tariff_code
, power_kw
, status_id
, client_type_id
, last_contract_flag
, zipcode
FROM eae.con_contract_dim) a
left join (
SELECT client_type_id, client_type_description
 FROM eae.con_client_type_dim) b
on a.client_type_id=b.client_type_id
 