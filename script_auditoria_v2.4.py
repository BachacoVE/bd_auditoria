#!/usr/bin/python 

import psycopg2

########################## Explicacion #################################
# Este script crea una base datos de replica de otra, para usarla como
# BD de auditoria. Este script genera un archivo "auditoria.sql"

### Datos de Configuracion de Conexion de la Base de Datos Fuente ###
# RECUERDE MODIFICAR LOS PARAMETROS DE CONEXION
database='f2015-des' #cambiar por el que corresponda
host='localhost'
user='postgres'
password='123456'
###################################################

#### Datos de conexion a bases de datos destino (auditoria) (INCOMPLETO)
#dbname_destino = 'aud_%s' % (database)
#host_destino = 'localhost'
user_destino = 'auditoria' #este rol debe estar creado
password_destino = '4ud1t0r14'


########################################################################
listbs = ['base_language_export',
'base_language_import',
'base_language_install',
'base_module_configuration',
'base_module_import',
'base_module_update',
'base_module_upgrade',
'base_setup_terminology',
'base_update_translations',
'board_board',
'board_board_line',
'board_menu_create',
'ir_act_client',
'ir_actions',
'ir_actions_configuration_wizard',
'ir_actions_todo',
'ir_actions_todo_category',
'ir_act_report_custom',
'ir_act_report_xml',
'ir_act_server',
'ir_act_url',
'ir_act_window',
'ir_act_window_group_rel',
'ir_act_window_view',
'ir_act_wizard',
'ir_attachment',
'ir_config_parameter',
'ir_cron',
'ir_default',
'ir_exports',
'ir_exports_line',
'ir_filters',
'ir_mail_server',
'ir_model',
'ir_model_access',
'ir_model_data',
'ir_model_fields',
'ir_model_fields_group_rel',
'ir_module_category',
'ir_module_module',
'ir_module_module_dependency',
'ir_property',
'ir_rule',
'ir_sequence',
'ir_sequence_type',
'ir_server_object_lines',
'ir_translation',
'ir_ui_menu',
'ir_ui_menu_group_rel',
'ir_ui_view',
'ir_ui_view_custom',
'ir_ui_view_sc',
'ir_values',
'ir_wizard_screen',
'maintenance_contract',
'migrade_application_installer_modules',
'multi_company_default',
'osv_memory_autovacuum',
'partner_clear_ids',
'partner_massmail_wizard',
'partner_sms_send',
'partner_wizard_ean_check',
'product_installer',
'publisher_warranty_contract',
'publisher_warranty_contract_wizard',
'rel_modules_langexport',
'rel_server_actions',
'res_bank',
'res_company',
'res_company_users_rel',
'res_config',
'res_config_installer',
'res_country',
'res_country_state',
'res_currency',
'res_currency_rate',
'res_currency_rate_type',
'res_groups',
'res_groups_action_rel',
'res_groups_implied_rel',
'res_groups_report_rel',
'res_groups_users_rel',
'res_groups_wizard_rel',
'res_lang',
'res_log',
'res_log_report',
'res_partner',
'res_partner_address',
'res_partner_bank',
'res_partner_bank_type',
'res_partner_bank_type_field',
'res_partner_category',
'res_partner_category_rel',
'res_partner_event',
'res_partner_title',
'res_payterm',
'res_request',
'res_request_history',
'res_request_link',
#'res_users',
'res_widget',
'res_widget_user',
'res_widget_wizard',
'rule_group_rel',
'user_preferences_config',
'wizard_ir_model_menu_create',
'wizard_ir_model_menu_create_line',
'wkf',
'wkf_activity',
'wkf_instance',
'wkf_logs',
'wkf_transition',
'wkf_triggers',
'wkf_witm_trans',
'wkf_workitem',
#Excluir vistas
'task_by_days']
omite = ''
for listb in listbs:
    exluye = " AND table_name<>'%s'" % listb
    omite+=exluye
########################################################################
conn = psycopg2.connect("dbname=%s host=%s user=%s password=%s" % (database, host, user, password))
cur_tabla = conn.cursor()
###Consulta que permite obtener los nombres de las tables de la base de datos especificada
cur_tabla.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'" + omite + "ORDER BY table_name")

###conexion con la base de datos de auditoria, recuerde que ya debe estar creada (de forma manual)
#aud_conn = psycopg2.connect("dbname=aud_%s host=%s user=%s password=%s" % (database, host, user, password))

###Se abre el archivo con el que se va a trabajar
sAuditable = open('script_bd_auditoria.sql', 'w')
sTrigger = open('script_trigger_auditoria.sql', 'w')
sBash = open('audiBash.sh', 'w')

sAuditable.write("""CREATE DATABASE aud_%s
  WITH OWNER = openerp
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'es_VE.UTF-8'
       LC_CTYPE = 'es_VE.UTF-8'
       CONNECTION LIMIT = -1;""" % (database)) # se crea la nueva base de datos con el prefijo 'aud_'

sAuditable.write("""\n\c aud_%s \n CREATE EXTENSION dblink;"""% (database))
sTrigger.write("""\c %s \n CREATE EXTENSION dblink;"""% (database))  

for tabla in cur_tabla:

	###====================================== Creacion de las Tablas ======================================
	sAuditable.write("""

CREATE TABLE %s
(
  audit_id serial NOT NULL,								-- PK: Identificador autoincremental del registro historico de auditoria
  audit_datetime timestamp without time zone,			-- Fecha y Hora de la creacion del registro
  audit_user character varying(50),						-- Codigo SISR del usuario que realizo la operacion
  audit_oper character varying(50),						-- Codigo del tipo de operacion (Insert, Update, Delete)
""" % (tabla))
	cur_columna = conn.cursor()

	### Consulta que permite obtener el nombre, tipo y longitud de cada campo de la tabla en revision.
	cur_columna.execute("select column_name, data_type, character_maximum_length from information_schema.columns where table_name='%s' ORDER BY ordinal_position ASC" % (tabla))
	i = 0
	cols = []
	for columna in cur_columna:
		cols.append(columna)
	for columna in cols:
		i = i + 1
		if(i == len(cols)):
			if columna[2] == None:
				sAuditable.write("""  %s %s
	""" % (columna[0], columna[1]))
			else:
				sAuditable.write("""  %s %s(%s)
	""" % (columna[0], columna[1], columna[2]))
		else:
			if columna[2] == None:
				sAuditable.write("""  %s %s,
	""" % (columna[0], columna[1]))
			else:
				sAuditable.write("""  %s %s(%s),
	""" % (columna[0], columna[1], columna[2])) 
		
	sAuditable.write(""");""")
	

	###==================================== Creacion del StoreProcedure ===================================		
	cur_columna.execute("SELECT column_name FROM information_schema.columns WHERE table_name='%s' ORDER BY ordinal_position ASC" % (tabla))
	columnas = ""
	new_columnas = ""
	old_columnas = ""
	i = 0	
	cols = []
	for columna in cur_columna:
		cols.append(columna)
	
	for columna in cols:
		i = i + 1
		if (columnas == ""):
			columnas = columna[0]
			new_columnas = "'''||NEW." + columna[0]+"||'''"
			old_columnas = "'''||OLD." + columna[0]+"||'''"
		else:
			if (i % 4 == 0):
				if (i < len(cols)):
					columnas = columnas + ", " + columna[0] +  ", " + '\n\t\t'
					new_columnas = new_columnas + ", " + "'''||NEW." + columna[0] + "||'''" + ", " + '\n\t\t'
					old_columnas = old_columnas + ", " + "'''||OLD." + columna[0] + "||'''" + ", " +  '\n\t\t'
				else:
					columnas = columnas + ", " + columna[0] + '\n\t\t'
					new_columnas = new_columnas + ", " + "'''||NEW." + columna[0] +"||'''" + '\n\t\t'
					old_columnas = old_columnas + ", " + "'''||OLD." + columna[0] + "||'''" + '\n\t\t'					
			else:
				if (i % 4 == 1):
					columnas = columnas + columna[0]
					new_columnas = new_columnas + "'''||NEW." + columna[0] + "||'''"
					old_columnas = old_columnas + "'''||OLD." + columna[0] + "||'''"
				else:
					columnas = columnas + ", " + columna[0]
					new_columnas = new_columnas + ", " + "'''||NEW." + columna[0] + "||'''"
					old_columnas = old_columnas + ", " + "'''||OLD." + columna[0] + "||'''"
	sTrigger.write("""

-- Function: rau_%s()
-- DROP FUNCTION rau_%s();

CREATE OR REPLACE FUNCTION rau_%s()
  RETURNS TRIGGER AS $rau_%s$
	DECLARE
	BEGIN
	  PERFORM dblink_connect('dbname=aud_%s host=127.0.0.1 user=%s password=%s');
	-- Funcion para conservar un registro historico (auditoria) de todos los 
	-- cambios realizados a los datos en un tabla (sean por INSERT, UPDATE o DELETE)
	IF (TG_OP = 'INSERT') THEN
		IF NEW.write_date IS NULL THEN NEW.write_date = NEW.create_date; END IF;
		IF NEW.write_uid IS NULL THEN NEW.write_uid = NEW.create_uid; END IF;
		PERFORM dblink_exec('INSERT INTO %s (audit_datetime, audit_user, audit_oper,
		%s)
		VALUES (
		NOW(), '''||current_user||''', '''||TG_OP||''',
		%s);');
		PERFORM dblink_disconnect();
	END IF;

	IF (TG_OP = 'UPDATE') THEN
		PERFORM dblink_exec('INSERT INTO %s (audit_datetime, audit_user, audit_oper,
		%s)
		VALUES (               
		NOW(), '''||current_user||''', '''||TG_OP||''',
		%s);');
		PERFORM dblink_disconnect();
	END IF;

	IF (TG_OP = 'DELETE') THEN
		IF OLD.write_date IS NULL THEN OLD.write_date = OLD.create_date; END IF;
		IF OLD.write_uid IS NULL THEN OLD.write_uid = OLD.create_uid; END IF;
		PERFORM dblink_exec('INSERT INTO %s (audit_datetime, audit_user, audit_oper,
		%s)
		VALUES (               
		NOW(), '''||current_user||''', '''||TG_OP||''',
		%s);');
		PERFORM dblink_disconnect();
	END IF;

	RETURN NEW;
END;
$rau_%s$ LANGUAGE plpgsql;
""" % (tabla[0], tabla[0], tabla[0],tabla[0],
	database,user_destino, password_destino,tabla[0], columnas, new_columnas,
	tabla[0], columnas, new_columnas,
	tabla[0], columnas, old_columnas,
	tabla[0]))

	###====================================== Creacion del Triger =====================================
	sTrigger.write("""

-- Trigger: rau_%s on %s
-- DROP TRIGGER rau_%s ON %s;

CREATE TRIGGER rau_%s
  AFTER INSERT OR UPDATE OR DELETE
  ON %s
  FOR EACH ROW
  EXECUTE PROCEDURE rau_%s();""" % (tabla[0], tabla[0], tabla[0], tabla[0], tabla[0], tabla[0], tabla[0]))

sBash.write("""#!/bin/bash 
if [ $(whoami) = "postgres" ];then
    psql -f  `dirname $0`/script_bd_auditoria.sql &&  psql -f `dirname $0`/script_trigger_auditoria.sql ;  echo "Ingrese la clave de root para reiniciar openerp" && su root /etc/init.d/openerp restart 
else
    su postgres
fi""") 
cur_columna.close
cur_tabla.close
conn.close

