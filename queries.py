eventos_query = """with mun as (
select
	p."DESCRICAO" as "PAIS",
	m."MUNICIPIO_ID",
	m."MUNICIPIO",
	case
		when m."UF_ID" = 'EX' then (
		select
			p."PROVINCIA"
		from
			cep.provincia p
		where
			p."PROVINCIA_ID" = m."PROVINCIA_ID")
		else "UF_ID"
	end as "UF_PROV"
from
	cep.municipio m
left join cep.pais p on
	m."PAIS_ID" = p."PAIS_ID"
),muni as(
select 
	r."REFERENCIA_ID", 
	r."REFERENCIA", 
	mun."PAIS", 
	mun."MUNICIPIO", 
	mun."UF_PROV" from oper.referencia r 
	join mun on r."MUNICIPIO_ID" = mun."MUNICIPIO_ID")
select
	eo."CONTROLE_EVO_ID",
	case
		eo."NUM_EVENTO" when 2 then 'Carregado'
		when 5 then 'Vazio'
		when 12 then 'Engate'
		when 13 then 'Desengate'
		when 9 then 'Recebimento'
		when 10 then 'Saída'
	end as "EVENTO",
	eo."DATA",
	eo."PLACA",
	eo."REFERENCIA_ID",
	re."NUM_ROMANEIO",
	eo."COD_PESSOA",
	pa."RAZAO_SOCIAL",
	r."PLACA_REFERENCIA",
	split_part(m."MUNICIPIO", '-', 1) as "MUNICIPIO",
	m."UF_PROV",
	m."PAIS"
from oper.evento_operacao eo
join oper.romaneio_evento re on
	re."CONTROLE_EVO_ID" = eo."CONTROLE_EVO_ID"
left join kss.pessoa pa on pa."COD_PESSOA" = eo."COD_PESSOA"
left join oper.romaneio r on r."NUM_ROMANEIO" = re."NUM_ROMANEIO"
left join muni m on m."REFERENCIA_ID" = eo."REFERENCIA_ID"
where eo."DATA">='2024-12-01' and 
	eo."NUM_EVENTO" in (2, 5, 12, 13, 9, 10) and r."PLACA_REFERENCIA" is not null """

rank_frota_query = """with mun as (
select
	p."DESCRICAO" as "PAIS",
	m."MUNICIPIO_ID",
	m."MUNICIPIO",
	case
		when m."UF_ID" = 'EX' then (
		select
			p."PROVINCIA"
		from
			cep.provincia p
		where
			p."PROVINCIA_ID" = m."PROVINCIA_ID")
		else "UF_ID"
	end as "UF_PROV"
from
	cep.municipio m
left join cep.pais p on
	m."PAIS_ID" = p."PAIS_ID"
),muni as(
select 
	r."REFERENCIA_ID", 
	r."REFERENCIA", 
	mun."PAIS", 
	mun."MUNICIPIO", 
	mun."UF_PROV" from oper.referencia r 
	join mun on r."MUNICIPIO_ID" = mun."MUNICIPIO_ID")
select distinct
	rf."PLACA_REFERENCIA",
	rf."PLACA_CONTROLE",
	rf."NUM_ROMANEIO",
	rf."REFERENCIA_ID",
	rf."DIA",
	rf."ENGATADA",
	case
		when "MODALIDADE_REFERENCIA" is null then "MODALIDADE_CONTROLE"
		else "MODALIDADE_REFERENCIA"
	end as "MODALIDADE",
	rf."NOME_MOTORISTA",
	case "COM_MOTORISTA"::text when '2' then 'Sim' when '1' then 'Não' when '0' then 'Não' end as "COM_MOTORISTA",
	rf."STATUS",
	rf."OPERACAO_REDUZIDA",
	rf."DATA_TERMINO",
	split_part(m."MUNICIPIO", '-', 1) as "MUNICIPIO_ATUAL",
	m."UF_PROV"as "UFP_ATUAL",
	m."PAIS" as "PAIS_ATUAL"
from
	oper.rank_frota rf 
	left join oper.romaneio r2 	on rf."NUM_ROMANEIO" = r2."NUM_ROMANEIO"
	left join customizacoes_932.funcionario f on f."COD_PESSOA" = rf."COD_PESSOA"
	left join muni m on m."REFERENCIA_ID" = rf."REFERENCIA_ID"
where
   rf."DIA" is not null and ("MODALIDADE_CONTROLE" = 'FROTA' or "MODALIDADE_REFERENCIA"= 'FROTA') and rf."STATUS" = 'Vazio' and rf."COM_MOTORISTA" = 2"""

classificados_query = """select distinct
	v."COD_VEICULO" as "PLACA",
	case  vtc."DESCRICAO"  When 'SEMI REBOQUE BAU - 3 EIXOS DIST' Then 'BA'
                    When 'SEMI REBOQUE SIDER - 3 EIXOS DIST' Then 'SD'
                    When 'SEMI REBOQUE SIDER - 2 EIXOS DIST' Then 'SD'
                    When 'SEMI REBOQUE SIDER BITREM DIANT - 3 EIXOS 1 E 2 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER BITREM DIANT - 3 EIXOS 1 E 3 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER BITREM DIANT - 3 EIXOS 1 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER BITREM TRAZ - 3 EIXOS 1 E 2 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER BITREM TRAZ - 3 EIXOS 1 E 3 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER BITREM TRAZ - 3 EIXOS 1 SUSP' Then 'RO'
                    When 'SEMI REBOQUE SIDER RODOTREM - 2 EIXOS 1 SUSP' Then 'RO'
					When 'CAVALO MECANICO 4X2' Then '4X2'
					When 'CAVALO MECANICO 6X2' Then '6X2'
					When 'CAVALO MECANICO 6X4' Then '6X4'
					When 'CAVALO MECANICO 4X2 - BAU' Then '4X2B'
					When 'CAVALO MECANICO 6X2 - AGREGADO' Then '6X2A'
					When 'SEMI REBOQUE TANQUE - 3 EIXOS DISTANCIADOS' Then 'TANQUE'
					else VTC."DESCRICAO"
       end as "TIPO_CARRETA",
       	case va."VALOR"  When 'Sim' Then 'C'
                    When 'Não' Then 'N'
                    end as "CLASSIFICADO"
	from veiculo.veiculo_atributo va  
	left join veiculo.veiculo v 
    on va."PLACA" = v."PLACA"
	left join veiculo.veiculo_tipo_carroceria vtc 
	on v."TIPO_CARROCERIA_ID" = vtc."TIPO_CARROCERIA_ID"
	where va."COD_ATRIBUTO" = 'TRANSPORTA_QUIMICO_CLASSIFIC' and v."COD_VEICULO" is not null"""

mopp_query = """select distinct "MOT_MOPP_ID","COD_PESSOA","DATA_VENCIMENTO" from oper.motorista_mopp mm """

updated_query = """with updt as(select MAX("DATA_CARGA") as "DATA_CARGA" from oper.evento_operacao eo where "DATA_CARGA">='2025-05-01'
union all 
select MAX("DATA_CARGA") as "DATA_CARGA"  from oper.romaneio r where "DATA_CARGA">='2025-05-01'
union all
select MAX("DATA_CARGA") as "DATA_CARGA" from oper.rank_frota rf where "DATA_CARGA">='2025-05-01')select MIN("DATA_CARGA") as "DATA_CARGA" from updt"""
