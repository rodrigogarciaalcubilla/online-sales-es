WITH USERS AS 
(
  SELECT 
    users.* except(notes,users.created_at,updated_at,external_id,tags,id,users.organization_id,users.url),
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

    ,tickets.* except(channel)--except(user_created_at,user_updated_at,updated_at,last_login_at),
    , case when channel = "api" and tickets.description ilike "%aircall%" then "aircall" else tickets.channel end as channel
    , case 
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__ccpp' then 'documentation request'
        when lower(tipo_de_incidencia) = 'solicitud_información__ya_cliente' then 'information request'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__partes_amistosos' then 'documentation request'
        when lower(tipo_de_incidencia) = 'cotización__contratada' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__proceso_prima' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__precio' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__fraccionamiento_pago' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) ilike "%solicitud_información__cliente_potencial%" then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__pago' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__documentación' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__sobre_prima' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__cancelación_competencia' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__coberturas__añadir_garantias' then 'purchase support'
        WHEN LOWER(Tipo_de_incidencia) = 'solicitud_información__cliente_potencial__coberturas__información_de_producto' then 'purchase support'
        when lower(tipo_de_incidencia) = 'fiva' then 'others'
        when lower(tipo_de_incidencia) = 'interrupción__competencia' then 'interruptions'
        when lower(tipo_de_incidencia) = 'interrupción__decisión_prima__declaración_inexacta' then 'interruptions'
        when lower(tipo_de_incidencia) = 'modificación__no_interrumpimos' then 'interruptions'
        when lower(tipo_de_incidencia) = 'cotización__flujo_incompleto__oferta_no_finalizada' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__cobertura/producto' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__interesado' then 'purchase support'
        when lower(tipo_de_incidencia) = 'modificación__no_interrumpimos' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'solicitud_información__cliente_potencial' then 'purchase support'
        when lower(tipo_de_incidencia) = 'otros' then 'others'
        when lower(tipo_de_incidencia) = 'proveedores__siniestros' then 'providers'
        when lower(tipo_de_incidencia) = 'cotización__flujo_incompleto__datos_incorrectos' then 'purchase support'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__datos_personales' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'qyr' then 'complaints'
        when lower(tipo_de_incidencia) = 'prima_pide_documentación__acepta' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'cotización__no_aparece_prima' then 'purchase support'
        when lower(tipo_de_incidencia) = 'prima_pide_documentación__no_localizado' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'prima_pide_documentación__recibida' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__competencia' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada' then 'purchase support'
        when lower(tipo_de_incidencia) = 'interrupción__formulario_recibido' then 'interruptions'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__fecha_de_entrada_en_vigor' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__pago_tarjeta' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__otros' then 'purchase support'
        when lower(tipo_de_incidencia) = 'interrupción__no_compra_el_vehículo' then 'interruptions'
        when lower(tipo_de_incidencia) = 'interrupción__deja_sin_seguro' then 'interruptions'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__normas_de_suscripción' then 'purchase support'
        when lower(tipo_de_incidencia) = '' then 'others'
        when lower(tipo_de_incidencia) = 'trustpilot' then 'feedback request'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__fraccionamiento_pago' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__cobertura/producto' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_localizado' then 'purchase support'
        when lower(tipo_de_incidencia) = 'interrupción__venta' then 'interruptions'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__competencia' then 'purchase support'
        when lower(tipo_de_incidencia) = 'encuestas__no_localizado' then 'feedback request'
        when lower(tipo_de_incidencia) = 'encuestas__realizada' then 'feedback request'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__interesado' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__contratado' then 'purchase support'
        when lower(tipo_de_incidencia) = 'proveedores__asistencia_en_viaje' then 'providers'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__datos_incorrectos' then 'purchase support'
        when lower(tipo_de_incidencia) = 'interrupción__baja_en_dgt' then 'interruptions'
        when lower(tipo_de_incidencia) = 'cotización__oferta_caducada' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__recibo' then 'documentation request'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__matrícula' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'interrupción__normas_de_suscripción' then 'interruptions'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__datos_vehículos' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__precio' then 'purchase support'
        when lower(tipo_de_incidencia) = 'prima_pide_documentación__no_acepta' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__certificado_siniestralidad' then 'documentation request'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__carta_verde' then 'documentation request'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__fecha_de_pago' then 'purchase support'
        when lower(tipo_de_incidencia) = 'interrupción__cobertura/producto' then 'interruptions'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__otro' then 'documentation request'
        when lower(tipo_de_incidencia) = 'proveedores__inbound' then 'providers'
        when lower(tipo_de_incidencia) = 'interrupción__insatisfecho_servicio' then 'interruptions'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__proceso_prima' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__pago_tarjeta' then 'purchase support'
        when lower(tipo_de_incidencia) = 'privacidad' then 'others'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__ccgg' then 'documentation request'
        when lower(tipo_de_incidencia) = 'prima_pide_documentación__solicitada_por_email' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'interrupción__decisión_prima__agravación_del_riesgo' then 'interruptions'
        when lower(tipo_de_incidencia) = 'interrupción__incremento_de_precio' then 'interruptions'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__fecha_de_pago' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado' then 'purchase support'
        when lower(tipo_de_incidencia) = 'cotización__flujo_incompleto__no_tiene_5_dígitos_sinco_ok' then 'purchase support'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__otros' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__no_contratable_sinco' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__otros' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__no_tiene_5_dígitos_sinco_ok' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__no_contratable_por_sinco' then 'purchase support'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__oficina_física' then 'purchase support'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'encuestas__no_interesado' then 'feedback request'
        when lower(tipo_de_incidencia) = 'google_reseñas' then 'feedback request'
        when lower(tipo_de_incidencia) = 'google' then 'feedback request'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__ipid' then 'documentation request'
        when lower(tipo_de_incidencia) = 'bandeja_de_ofertas__no_contratado__ns/nc' then 'purchase support'
        when lower(tipo_de_incidencia) = 'modificación__interrumpimos__conductores_no_declarados' then 'request for amendment'
        when lower(tipo_de_incidencia) = 'llamada_perdida__no_contesta' then 'others'
        when lower(tipo_de_incidencia) = 'renovación' then 'renewals'
        when lower(tipo_de_incidencia) = 'confirmación_datos_contacto' then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__oficina_física' then 'purchase support'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__otros' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__decisión_de_prima' then 'renewals'
        when lower(tipo_de_incidencia) = 'interrupción__fallecimiento_del_tomador' then 'interruptions'
        when lower(tipo_de_incidencia) = 'renovación__solicita_precio__no_disponible' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__solicita_precio__no_conforme' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__incremento_de_precio' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__competencia' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__deja_sin_seguro' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__solicita_precio__lo_pensará' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__solicita_precio__conforme' then 'renewals'
        when lower(tipo_de_incidencia) = 'cotización__no_contratada__ns/nc' then 'purchase support'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__insatisfecho_servicio' then 'renewals'
        when lower(tipo_de_incidencia) = 'renovación__no_renueva__venta' then 'renewals'
        when lower(tipo_de_incidencia) = 'interrupción__pérdida_total/robo' then 'interruptions'
        when lower(tipo_de_incidencia) ilike any('%cotización%','%cotizacion%','%bandeja_de_ofertas%') then 'purchase support'
        when lower(tipo_de_incidencia) ilike any('%renovación%','%renovacion%') then 'renewals'
        when lower(tipo_de_incidencia) ilike any('%información%','%informacion%') then 'information request'
        when lower(tipo_de_incidencia) ilike any('%interrupción%','%interrupcion%','%interrumpimos%') then 'interruptions'
        when lower(tipo_de_incidencia) ilike any('%prima_pide%','%prima_pide_documentación%','%prima_pide_documentacion%') then 'prima requests documentation/information'
        when lower(tipo_de_incidencia) ilike any('%cliente_pide%','%cliente_pide_documentación%','%cliente_pide_documentacion%') then 'documentation request'
        when lower(tipo_de_incidencia) ilike any('%trustpilot%') then 'feedback request'
        when lower(tipo_de_incidencia) = 'cliente_pide_documentación__ccpp' then 'documentation request'
        when lower(tipo_de_incidencia) = 'solicitud_información__ya_cliente' then 'information request'
        when lower(tipo_de_incidencia) in('cliente_pide_documentación__partes_amistosos','prima_pide_documentación__acepta') then 'documentation request'
        when lower(tipo_de_incidencia) in('cotización__contratada',
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
                                          'bandeja_de_ofertas__no_contratado__interesado',
                                          'bandeja_de_ofertas__datos_incorrectos') then 'purchase support'
        when lower(tipo_de_incidencia) = 'fiva' then 'others'
        when lower(tipo_de_incidencia) in('interrupción__competencia',
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
                                          'interrupción__pérdida_total/robo') then 'interruptions'
        when lower(tipo_de_incidencia) in('proveedores__siniestros','proveedores__asistencia_en_viaje') then 'providers'
        when lower(tipo_de_incidencia) ilike '%proveedor%' then 'providers'
        when lower(tipo_de_incidencia) ilike '%solicitud_información%' then 'information request'
        when lower(tipo_de_incidencia) in('modificación__no_interrumpimos','modificación__interrumpimos__fecha_de_entrada_en_vigor') then 'amendments'
        when lower(tipo_de_incidencia) in('chat__chat_sin_respuesta','error_en_la_llamada','llamada_perdida__no_contesta') then 'cliente no responde'
        else 'others'
    end as incidencia_grouped,

    case when tags ilike "% unseen%" then "unseen" when tags ilike "% seen%" then "seen" else null end as mail_seen, 
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
  -- and incidencia_grouped =  "purchase support"
  and (purchased_at - INTERVAL 3 DAYS) <= zendesk_full.created_at -- esta es la clave

  order by zendesk_full.created_at
),

recontact as (
select 
    *,
    count(*) over (PARTITION BY requester_id) as num_contacts,
    sum(if(lag(created_at) over user_window is null or date_diff(DAY, lag(created_at) over user_window, created_at) > 15, 1, 0)) over user_window as n_contact_15,
    date_diff(DAY, lag(created_at) over user_window, created_at) as date_diff_lag,
    lag(created_at) over user_window as lag,
    -- created_at as created_at_aux,
    concat(requester_id,'-',n_contact_15) as client_15

--select * 
from zendesk_full
where channel <> "api"
-- where client_number = "34622833576"

window user_window AS (PARTITION BY requester_id ORDER BY created_at)
)-- select * from recontact where requester_id = 7504014611997
,
recontact_date as (
  select 
  id,
  num_contacts,
  n_contact_15,
  date_diff_lag,
  lag,
  client_15,
  min(created_at) over (partition by client_15 ) as first_contact_date_15,
  count(*) over (partition by client_15 ) as Num_contacts_15,
  first_value(assignee_id) over (partition by client_15 order by created_at) as first_agent_client_15,
  first_value(channel) over (partition by client_15 order by created_at) as first_channel_client_15,
  first_value(tipo_de_incidencia) over (partition by client_15 order by created_at) as first_tipo_de_incidencia_client_15,
  first_value(incidencia_grouped) over (partition by client_15 order by created_at) as first_incidencia_grouped_client_15


  from recontact 
) --select * from recontact_date

select 

* except (zendesk_ticket_metrics.updated_at,ingested_at,zendesk_ticket_metrics.created_at,zendesk_full.updated_at), 
greatest(zendesk_ticket_metrics.updated_at,zendesk_full.updated_at) as updated_at,
  regexp_extract(satisfaction_rating, 'score=(\\w+)', 1) AS Score,
  regexp_extract(satisfaction_rating, 'reason=([^,]+)', 1) AS Reason


from zendesk_full
left join recontact_date using(id)
left join cc_conversion_3d_criteria using(id)
left join hive_metastore.es_pricing_gold_production.zendesk_ticket_metrics on zendesk_ticket_metrics.ticket_id = zendesk_full.id 

where not (subject in ('Declaración de un siniestro','Inbound answered call on S 2 - Seguimiento Siniestro','Inbound answered call on Numero Test Siniestros','Inbound answered call on S 1 - Apertura Siniestro')) --me quito contactos de sinistros
-- and channel actos = "aircall"
-- and subject ilike "%Siniestro%"
