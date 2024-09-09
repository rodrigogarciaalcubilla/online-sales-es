WITH USERS AS 
(
  SELECT 
    users.* except(notes,users.created_at,updated_at,external_id,tags,id),
    users.id as requester_id, 
    users.created_at as user_created_at,
    users.updated_at as user_updated_at,
    upper(notes) as notes,
          -- Extract DNI/NIE using regular expressions and add it to a new column
          CASE
              WHEN upper(notes) REGEXP '\\b\\d{8}[A-Z]\\b' THEN REGEXP_SUBSTR(upper(notes), '\\b\\d{8}[A-Z]\\b')
              WHEN upper(notes) REGEXP '\\b[XYZ]\\d{7}[A-Z]\\b' THEN REGEXP_SUBSTR(upper(notes), '\\b[XYZ]\\d{7}[A-Z]\\b')
              ELSE NULL
          END AS dni_nie,
          -- Determine the type of ID based on the structure and add it to a new column
          CASE
              WHEN upper(notes) REGEXP '\\b\\d{8}[A-Z]\\b' THEN 'DNI'
              WHEN upper(notes) REGEXP '\\b[XYZ]\\d{7}[A-Z]\\b' THEN 'NIE'
              ELSE NULL
        END AS type_of_id

  from hive_metastore.es_pricing_gold_production.zendesk_users as users
  
  -- where len(notes) > 9 
  --   DNI (Documento Nacional de Identidad): This is the National ID Document for Spanish citizens. The structure of a DNI is 8 numerical digits followed by a letter. Here's an example structure: 12345678A.

  -- NIE (Número de Identidad de Extranjero): This is the identification number for foreigners. The structure of an NIE starts with a letter (X, Y, or Z), followed by 7 numerical digits and another letter at the end. Here's an example structure: X1234567B.
)  --SELECT len(noteS),* FROM USERS where type_of_id is null and upper(notes) is not null and notes <>""
,


dni_prima_data AS 
(
  select 
  upper(policyholder_dni) as dni_nie,
  case when len((policy_number)) > 5 then 1 else 0 end as purchased_policy,
  case when len((policy_number)) > 5 and (policy_interruption.occurred_on) is null then 1 else 0 end as policy_not_interrupted,
  (purchased_at) as purchased_at,
  (policy_number) as policy_number,
  policy_interruption.occurred_on as interrupted_at,
  (transaction.purchase_channel) as bo_purchase,
  source as pcw_source,
  renewal_order

  from hive_metastore.es_be_core_silver_production.customer_data
  -- left join es_be_core_silver_production.offer using(application_id)
  left join hive_metastore.es_be_core_silver_production.policy_view as policy USING(application_id)
  left join hive_metastore.es_be_core_silver_production.policy_interruption using(policy_id)
  left JOIN hive_metastore.es_be_core_silver_production.offer b ON policy.offer_id = b.offer_id
  left join hive_metastore.es_be_core_silver_production.quote using(application_id,quote_id)
  left JOIN payments_production.silver.es_received c ON c.offer_id = b.customer_facing_id
  left JOIN hive_metastore.es_be_core_silver_production.transaction using(order_id)

  -- where renewal_order = 0 --only new business
  -- group by 1 

)
,
---new zendesk join with policies
policies_purchased_by_ticket as (
select 
-- purchased_at,
-- created_at,
tickets.id as id,
case when purchased_at < created_at + interval 7  days then policy_number else null end as policy_number_7days,
case when purchased_at < created_at + interval 14  days then policy_number else null end as policy_number_14days,
case when purchased_at < created_at + interval 1  days then policy_number else null end as policy_number_1days,
case when purchased_at < created_at + interval 3  days then policy_number else null end as policy_number_3days,
abs(purchased_at - created_at) as dif_between_ticket_and_purchase,
case when interrupted_at < created_at + interval 7  days then policy_number else null end as interrupted_7days,
case when interrupted_at < created_at + interval 14  days then policy_number else null end as interrupted_14days,
case when interrupted_at < created_at + interval 1  days then policy_number else null end as interrupted_1days,
case when interrupted_at < created_at + interval 3  days then policy_number else null end as interrupted_3days,
abs(interrupted_at - created_at) as dif_between_ticket_and_interrupted,

  -- DATEDIFF(purchased_at, created_at) AS days_difference_between_ticket_and_purchase,
  FLOOR((UNIX_TIMESTAMP(purchased_at) - UNIX_TIMESTAMP(created_at))/ 86400) AS days_difference,
    FLOOR((UNIX_TIMESTAMP(interrupted_at) - UNIX_TIMESTAMP(created_at))/ 86400) AS days_difference_interruption,
-- bo_purchase,
dni_prima_data.*
-- select * 
from hive_metastore.es_pricing_gold_production.zendesk_tickets as tickets
left join users using(requester_id)
LEFT JOIN dni_prima_data using(dni_nie)

where true 
and purchased_at >= created_at 
)
,
--por si hay duplicados de 2 polizas para un dni
policies_by_ticket_unqiue as 
(
  select *

  from policies_purchased_by_ticket
  join (select id, min(dif_between_ticket_and_purchase) as dif_between_ticket_and_purchase from policies_purchased_by_ticket group by 1 ) using(id,dif_between_ticket_and_purchase)

) --select * from policies_by_ticket_unqiue
,

zendesk_full as (
  select 
      replace(REPLACE(
        REGEXP_EXTRACT(tickets.description, 'Caller Phone number : ([+0-9 ]+)', 1),
        ' ','')
        ,'+34','')::int AS CallerPhoneNumber,
        right(REPLACE(phone, ' ', ''),9) as user_phone,
        coalesce(CallerPhoneNumber,user_phone) as zdk_phone_to_join
    ,UNIX_TIMESTAMP(tickets.created_at) as ticket_created_at_unix

    ,tickets.* --except(user_created_at,user_updated_at,updated_at,last_login_at),
     ,  case 
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__CCPP' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Solicitud_información__Ya_cliente' then 'Information request'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__Partes_amistosos' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__Contratada' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) ilike "%solicitud_información__cliente_potencial%" then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__pago' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__documentación' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__sobre_prima' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__cancelación_competencia' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__coberturas__añadir_garantias' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__coberturas__información_de_producto' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Proceso_Prima' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Precio' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Fraccionamiento_pago' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'FIVA' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Competencia' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Decisión_Prima__Declaración_inexacta' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'modificación__no_interrumpimos' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__Flujo_incompleto__Oferta_no_finalizada' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Cobertura/Producto' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Interesado' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__No_interrumpimos' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Solicitud_información__Cliente_potencial' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Otros' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'Proveedores__Siniestros' then 'Providers'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__Flujo_incompleto__Datos_incorrectos' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Datos_personales' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'QyR' then 'Complaints'
    WHEN LOWER(Tipo_de_incidencia) = 'Prima_pide_documentación__Acepta' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_aparece_Prima' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Prima_pide_documentación__No_localizado' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Prima_pide_documentación__Recibida' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Competencia' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__no_contratada' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Formulario_recibido' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Fecha_de_entrada_en_vigor' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Pago_tarjeta' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Otros' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__No_compra_el_vehículo' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Deja_sin_seguro' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Normas_de_suscripción' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = '' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'Trustpilot' then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Fraccionamiento_pago' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Cobertura/Producto' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_localizado' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Venta' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Competencia' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Encuestas__No_localizado' then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'Encuestas__Realizada' then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Interesado' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__Contratado' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Proveedores__Asistencia_en_viaje' then 'Providers'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__Datos_incorrectos' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Baja_en_DGT' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__Oferta_caducada' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__Recibo' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Matrícula' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Normas_de_suscripción' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Datos_vehículos' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Precio' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Prima_pide_documentación__No_acepta' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__Certificado_Siniestralidad' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__Carta_Verde' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Fecha_de_pago' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Cobertura/Producto' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__Otro' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Proveedores__Inbound' then 'Providers'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Insatisfecho_servicio' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Proceso_Prima' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Pago_tarjeta' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Privacidad' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__CCGG' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Prima_pide_documentación__Solicitada_por_email' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Decisión_Prima__Agravación_del_riesgo' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Incremento_de_precio' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Fecha_de_pago' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__Flujo_incompleto__No_tiene_5_dígitos_SINCO_ok' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Otros' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__No_contratable_SINCO' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Otros' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__No_tiene_5_dígitos_SINCO_ok' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__No_contratable_por_SINCO' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__Oficina_física' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__interrumpimos' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Encuestas__No_interesado' then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'Google_reseÃ±as' then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'Cliente_pide_documentación__IPID' then 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'Bandeja_de_ofertas__No_contratado__NS/NC' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Modificación__Interrumpimos__Conductores_no_declarados' then 'Request for amendment'
    WHEN LOWER(Tipo_de_incidencia) = 'Llamada_perdida__No_contesta' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'renovación' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Confirmación_datos_contacto' then 'Prima requests documentation/information'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__Oficina_física' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Otros' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Decisión_de_Prima' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Fallecimiento_del_tomador' then 'Interruptions'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__Solicita_precio__No_disponible' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__Solicita_precio__No_conforme' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Incremento_de_precio' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Competencia' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Deja_sin_seguro' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Error_en_la_llamada' then 'Others'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__Solicita_precio__Lo_pensará' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__Solicita_precio__Conforme' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Cotización__No_contratada__NS/NC' then 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Insatisfecho_servicio' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Renovación__No_renueva__Venta' then 'Renewals'
    WHEN LOWER(Tipo_de_incidencia) = 'Interrupción__Pérdida_total/Robo' then 'Interruptions'
    when LOWER(Tipo_de_incidencia) ilike any("%cotización%","%cotizacion%","%bandeja_de_ofertas%") then 'purchase support'
    when LOWER(Tipo_de_incidencia) ilike any("%renovación%","%renovacion%") then 'Renewals'
    when LOWER(Tipo_de_incidencia) ilike any("%información%","%informacion%") then 'Information request'
    when LOWER(Tipo_de_incidencia) ilike any("%interrupción%","%interrupcion%","%interrumpimos%") then 'Interruptions'
    when LOWER(Tipo_de_incidencia) ilike any("%prima_pide%","%prima_pide_documentación%","%prima_pide_documentacion%") then 'Prima requests documentation/information'
    when LOWER(Tipo_de_incidencia) ilike any("%cliente_pide%","%cliente_pide_documentación%","%cliente_pide_documentacion%") then 'Documentation request'
    when LOWER(Tipo_de_incidencia) ilike any("%trustpilot%") then 'Feedback request'
    WHEN LOWER(Tipo_de_incidencia) = 'cliente_pide_documentación__ccpp' THEN 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__ya_cliente' THEN 'Information request'
    WHEN LOWER(Tipo_de_incidencia) in('cliente_pide_documentación__partes_amistosos','prima_pide_documentación__acepta') THEN 'Documentation request'
    WHEN LOWER(Tipo_de_incidencia) IN ('cotización__contratada', 
                                       'cotización__no_contratada__proceso_prima',
                                       'cotización__no_contratada__precio',
                                       'cotización__no_contratada__fraccionamiento_pago',
                                       'cotización__flujo_incompleto__oferta_no_finalizada',
                                       'cotización__no_contratada__cobertura/producto',
                                       'cotización__no_contratada__interesado',
                                       'cotización__flujo_incompleto__datos_incorrectos',
                                       'cotización__no_aparece_prima',
                                       'cotización__no_contratada__competencia',
                                       'cotización__no_contratada',
                                       'cotización__no_contratada__pago_tarjeta',
                                       'cotización__no_contratada__otros',
                                       'cotización__no_contratada__normas_de_suscripción',
                                       'cotización__oferta_caducada',
                                       'cotización__no_contratada__fecha_de_pago',
                                       'cotización__no_contratada__oficina_física',
                                       'cotización__no_contratada__ns/nc',
                                       'bandeja_de_ofertas__no_contratado__no_tiene_5_dígitios_sinco_ok',
                                       "bandeja_de_ofertas__no_contratado__interesado",
                                       'bandeja_de_ofertas__datos_incorrectos') THEN 'purchase support'
    WHEN LOWER(Tipo_de_incidencia) = 'fiva' THEN 'Others'
    WHEN LOWER(Tipo_de_incidencia) IN ('interrupción__competencia',
                                       'interrupción__decisión_prima__declaración_inexacta',
                                       'interrupción__formulario_recibido',
                                       'interrupción__no_compra_el_vehículo',
                                       'interrupción__deja_sin_seguro',
                                       'interrupción__venta',
                                       'interrupción__baja_en_dgt',
                                       'interrupción__cobertura/producto',
                                       'interrupción__insatisfecho_servicio',
                                       'interrupción__decisión_prima__agravación_del_riesgo',
                                       'interrupción__incremento_de_precio',
                                       'interrupción__pérdida_total/robo') THEN 'Interruptions'
     WHEN LOWER(Tipo_de_incidencia) IN ('proveedores__siniestros','proveedores__asistencia_en_viaje') then "Providers"
     when lower(tipo_de_incidencia) ilike '%solicitud_información%' then 'information request'
     WHEN LOWER(Tipo_de_incidencia) IN ('modificación__no_interrumpimos','modificación__interrumpimos__fecha_de_entrada_en_vigor') then "Amendments"
    -- Add other mappings here following the same pattern
    ELSE 'Others'
    end as incidencia_grouped,
    policies_by_ticket_unqiue.* except (id),

    --fields to calculate later
    -- case when policy_number is not null then 1 else 0 end as purchased_policy,
    case when policy_number_7days is not null then 1 else 0 end as purchased_policy_7days,
    case when policy_number_14days is not null then 1 else 0 end as purchased_policy_14days,
    case when policy_number_3days is not null then 1 else 0 end as purchased_policy_3days,
    case when policy_number_1days is not null then 1 else 0 end as purchased_policy_1days
    
  --select *
  from hive_metastore.es_pricing_gold_production.zendesk_tickets as tickets
  left join users using(requester_id)
  LEFT JOIN policies_by_ticket_unqiue using(id)

  -- where tickets.channel = "api"
) --select * from zendesk_full
,
cc_conversion_3d_criteria as 
(
  select 
  id,
  first_value(assignee_id) over (PARTITION BY policy_number,renewal_order order by zendesk_full.created_at) as first_assignee_id_3_days_criteria


  from zendesk_full

  where not (subject in ('Declaración de un siniestro','Inbound answered call on S 2 - Seguimiento Siniestro','Inbound answered call on Numero Test Siniestros','Inbound answered call on S 1 - Apertura Siniestro')) --me quito contactos de sinistros
  -- and policy_number is not null
  and channel <> "api"
  and incidencia_grouped =  "purchase support"
   and ((purchased_at - INTERVAL 3 DAYS) <= zendesk_full.created_at or purchased_at is null) -- esta es la clave

  order by zendesk_full.created_at
),

aircall AS 
(

  select 
  created_time as date, 
  dayofweek(created_time) as dow, 
  --el numero NPS 
  case 
      when `from` = "34919012607" then `to` 
      when `to` = "34919012607" then `from` 
      when direction = "inbound" then `from` else `to` 
      end as client_number_calculated,

  CASE 
    WHEN number_name = 'Rastreator' THEN 'Ventas'
    WHEN number_name = 'Pagina Web' THEN 'Ventas'
    WHEN number_name LIKE 'NPS %' THEN 'NPS'  -- This handles all 'NPS' prefixed values
    WHEN number_name = 'PW 3 - Gestion poliza' THEN 'Gestion poliza'
    WHEN number_name = 'PW 1.1 - Cliente con presupuesto' THEN 'Ventas'
    WHEN number_name = 'Asistencia en Carretera' THEN 'Otros'
    WHEN number_name = 'Siniestros' THEN 'Otros'
    WHEN number_name = 'PW 1.2 - Cliente sin presupuesto' THEN 'Ventas'
    WHEN number_name = 'Pruebas 3' THEN 'NPS'
    WHEN number_name = 'Pruebas 2' THEN 'NPS'
    WHEN number_name = 'Inbound RACE' THEN 'Otros'
    WHEN number_name = 'PW 1 - Cliente potencial' THEN 'Ventas'
    WHEN number_name = 'Pruebas' THEN 'NPS'
    WHEN number_name = 'Inbound Proveedores' THEN 'Otros'
    WHEN number_name = 'Acierto' THEN 'Ventas'
    ELSE NULL  -- Or a default value if you have one

  END AS Classification,
    right(`from`,9)::int as from_to_join,
    duration_in_seconds- in_call_duration as wait_time,
        (UNIX_TIMESTAMP(answered_time)-UNIX_TIMESTAMP(created_time)) as wait_time_calc_with_dates,

    CONVERT_TIMEZONE('Europe/Madrid', 'UTC', created_time)::timestamp AS created_time_utc,
    CONVERT_TIMEZONE('Europe/Madrid', 'UTC', ended_time)::timestamp AS ended_time_utc,
    CONVERT_TIMEZONE('Europe/Madrid', 'UTC', answered_time)::timestamp AS answered_time_utc,
    UNIX_TIMESTAMP(created_time_utc) + coalesce(wait_time,0) AS updated_timestamp_unix,
    unix_timestamp(answered_time_utc) as answered_at_utc_unix,
  * 
    --select * 
  from hive_metastore.es_pricing_gold_production.aircall_calls
  -- where dayofweek(`date (TZ offset incl.)`) not in (7,1)
  where true
  and lower(was_answered_yes__no) ilike "%yes%" --llamadas no respondidas no generan ticket 
  -- and direction = "inbound" --llamdas outbound no general ticket.   -- no lo quito aun para que los recalls se calculen correctamente, luego me lo fundo al final
  and not(number_name in ("Asistencia en Carretera","Inbound Proveedores","Pruebas","Pruebas 2","Pruebas 3","Siniestros","Numero Test Siniestros"))
  and not (`from` = "34919017999" and `to` = "34919012607") --elimino llamadas desde nestra WEB hacia NPS
  -- and not (`from` = "anonymous" or  `to` = "anonymous")
  and not (`from` = "34910602845" or  `to` = "34910602845") --telefono de MSA
  and not (`from` = "34917374762" or  `to` = "34917374762") --telefono de MSA
  -- and call_id in( "1824874146","1824877650") --ticket id de esto en zdk 178824

)
,
recalls as (
select 
    *,
    count(*) over (PARTITION BY client_number_calculated) as num_calls,
    sum(if(lag(created_time_utc) over user_window is null or date_diff(DAY, lag(created_time_utc) over user_window, created_time_utc) > 15, 1, 0)) over user_window as n_call_15,
    sum(if(lag(created_time_utc) over user_window is null or date_diff(DAY, lag(created_time_utc) over user_window, created_time_utc) > 30, 1, 0)) over user_window as n_call_30,
    sum(if(lag(created_time_utc) over user_window is null or date_diff(DAY, lag(created_time_utc) over user_window, created_time_utc) > 60, 1, 0)) over user_window as n_call_60,
    date_diff(DAY, lag(created_time_utc) over user_window, created_time_utc) as date_diff_lag,
    lag(created_time_utc) over user_window as lag,
    created_time_utc,
    concat(client_number_calculated,'-',n_call_15) as client_15,
    concat(client_number_calculated,'-',n_call_30) as client_30,
    concat(client_number_calculated,'-',n_call_60) as client_60
--select * 
from aircall
-- where client_number = "34622833576"

window user_window AS (PARTITION BY client_number_calculated ORDER BY created_time_utc)
)

,
recall_date as (
  select 
  *
  ,
  min(created_time_utc) over (partition by client_15 ) as first_call_date_15,
  min(created_time_utc) over (partition by client_30 ) as first_call_date_30,
  min(created_time_utc) over (partition by client_60 ) as first_call_date_60,
  count(*) over (partition by client_30 ) as Num_calls_30,
  count(*) over (partition by client_15 ) as Num_calls_15,
  count(*) over (partition by client_60 ) as Num_calls_60,
  first_value(full_name) over (partition by client_30 order by created_time_utc) as first_user_client_30,
  first_value(number_name) over (partition by client_30 order by created_time_utc) as first_number_name_client_30,
    first_value(full_name) over (partition by client_15 order by created_time_utc) as first_user_client_15,
  first_value(number_name) over (partition by client_15 order by created_time_utc) as first_number_name_client_15,
    first_value(full_name) over (partition by client_60 order by created_time_utc) as first_user_client_60,
  first_value(number_name) over (partition by client_60 order by created_time_utc) as first_number_name_client_60

  from recalls 
)
,
joined_data as (
select 
    call_id as aircall_id,
    -- created_time_utc as aircall_created_utc,
    -- created_at as zendesk_created_at,
    abs(answered_at_utc_unix - ticket_created_at_unix) as dif_between_call_and_ticket,
    FROM_UNIXTIME(ticket_created_at_unix)::TIMESTAMP,ticket_created_at_unix,left(ticket_created_at_unix,9),
    
    zendesk_full.*
-- from zendesk_full
from recall_date as aircall
left join zendesk_full on (zendesk_full.zdk_phone_to_join = aircall.from_to_join)
-- left join zendesk_full on (zendesk_full.callerphonenumber = aircall.from_to_join and left(updated_timestamp_unix,8) = left(ticket_created_at_unix,8))


where true 
and date >= "2022-10-26" --fuera llamadas de test pre-apertura prima
and in_call_duration > 2 
and was_answered_yes__no = "Yes"
and direction = "inbound" --aqui es donde me quito las inbound, no hay ticket asoicado si no es una inbound call.
-- and from = "34691875034"
-- and abs(answered_at_utc_unix - ticket_created_at_unix) < 60
-- and zendesk_full.id::int is null
-- and aircall.dow is not null
order by created_time_utc desc
),
joined_deduplicate_aircall_db as (
select 
  row_number() over (partition by id order by dif_between_call_and_ticket) as ranking_ticket_id,
  * 
  
from joined_data
join (select aircall_id,min(dif_between_call_and_ticket) as dif_between_call_and_ticket from joined_data group by 1 ) aux using(aircall_id,dif_between_call_and_ticket) -- para quitar posibles dupicados
order by 1 desc,2
),
join_deduplicate_zendesk as 
(
  select 
  * 

  from joined_deduplicate_aircall_db

  where ranking_ticket_id = 1 
  and dif_between_call_and_ticket < 10000 --me quito mas de 2 horas
  order by dif_between_call_and_ticket desc

)

select  
*,
first_value(incidencia_grouped) over (partition by client_30 order by created_time_utc) as first_incidencia_30,
first_value(incidencia_grouped) over (partition by client_15 order by created_time_utc) as first_incidencia_client_15,
first_value(incidencia_grouped) over (partition by client_60 order by created_time_utc) as first_incidencia_client_60

from recall_date as aircall
left join join_deduplicate_zendesk on aircall.call_id = join_deduplicate_zendesk.aircall_id
left join cc_conversion_3d_criteria using(id)

where true 
and date >= "2022-10-26" --fuera llamadas de test pre-apertura prima
and in_call_duration > 2 
and was_answered_yes__no = "Yes"
and direction = "inbound" --aqui es donde me quito las inbound, no hay ticket asoicado si no es una inbound call.
