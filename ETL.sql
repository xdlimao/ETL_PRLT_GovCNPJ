use ETLTest

--tabelas
	--empresas
create table empresas (
	cnpj_basico char(8) not null,
	razao_social varchar(max) not null,
	natureza_juridica char(4) not null,
	qualificacao char(2) not null,
	capital_social money not null,
	porte_empresa char(2) null,
	ente_federativo varchar(max) null
)
drop table empresas

	--estabelecimentos
create table estabelecimentos (
	cnpj_basico char(8) not null, --1r
	cnpj_ordem char(4) not null, --2r
	cnpj_dv char(2) not null, --3r
	identificador_matriz_filial tinyint not null, --4
	nome_fantasia varchar(max) null, --5
	situacao_cadastral char(2) not null, --6
	data_situacao_cadastro date null, --7
	motivo_situacao_cadastro tinyint not null, --8
	cidade_exterior varchar(max) null, --9
	pais smallint null, --10
	data_inicio date not null, --11
	cnae_principal char(7) not null, --12
	cnae_secundario varchar(max) null, --13
	tipo_logradouro varchar(10) null, --14
	logradouro varchar(max) not null, --15
	numero varchar(10) not null, --16
	complemento varchar(max) null, --17
	bairro varchar(max) not null, --18
	cep char(8) not null, --19r
	uf char(2) not null, --20
	municipio smallint not null, --21
	ddd1 char(2) null, --22
	telefone1 varchar(9) null, --23
	ddd2 char(2) null, --24
	telefone2 varchar(9), --25
	dddfax char(2) null, --26
	fax varchar(9) null, --27
	email varchar(max) null, --28
	situacao_especial varchar(max) null, --29
	data_situacao_especial date null --30
)
drop table estabelecimentos

-- cast
	--empresas
insert into empresas (
	cnpj_basico, 
	razao_social, 
	natureza_juridica, 
	qualificacao,
	capital_social,
	porte_empresa,
	ente_federativo
	)
select
	cast(column1 as char) as cnpj_basico,
	column2 as razao_social,
	cast(column3 as char) as natureza_juridica,
	cast(column4 as char) as qualificacao,
	cast(column5 as money) as capital_social,
	cast(column6 as char) as porte_empresa,
	column7 as ente_federativo
from 
	tabela1 

	--estabelecimentos
insert into estabelecimentos (	
	cnpj_basico,
	cnpj_ordem,
	cnpj_dv,
	identificador_matriz_filial,
	nome_fantasia,
	situacao_cadastral,
	data_situacao_cadastro,
	motivo_situacao_cadastro,
	cidade_exterior,
	pais,
	data_inicio,
	cnae_principal,
	cnae_secundario,
	tipo_logradouro,
	logradouro,
	numero,
	complemento,
	bairro,
	cep,
	uf,
	municipio,
	ddd1,
	telefone1,
	ddd2,
	telefone2,
	dddfax,
	fax,
	email,
	situacao_especial,
	data_situacao_especial
	)
select
    cast(column1 as char(8)) as cnpj_basico,
    cast(column2 as char(4)) as cnpj_ordem,
    cast(column3 as char(2)) as cnpj_dv,
    cast(column4 as tinyint) as identificador_matriz_filial,
    column5 as nome_fantasia,
    cast(column6 as char(2)) as situacao_cadastral,
	try_convert(datetime, column7, NULL) as data_situacao_cadastro,
    cast(column8 as tinyint) as motivo_situacao_cadastro,
    column9 as cidade_exterior,
    cast(column10 as smallint) as pais,
    cast(column11 as date) as data_inicio,
    cast(column12 as char(7)) as cnae_principal,
    column13 as cnae_secundario,
    cast(column14 as varchar(10)) as tipo_logradouro,
    column15 as logradouro,
    cast(column16 as varchar(10)) as numero,
    column17 as complemento,
    column18 as bairro,
    cast(column19 as char(8)) as cep,
    cast(column20 as char(2)) as uf,
    cast(column21 as smallint) as municipio,
    cast(column22 as char(2)) as ddd1,
    cast(column23 as varchar(9)) as telefone1,
    cast(column24 as char(2)) as ddd2,
    cast(column25 as varchar(9)) as telefone2,
    cast(column26 as char(2)) as dddfax,
    cast(column27 as varchar(9)) as fax,
    column28 as email,
    column29 as situacao_especial,
    convert(date, column30)  as data_situacao_especial
from
	tabela2