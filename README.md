#Algunos apuntes

Para usar un rol distinto (auditoria) al de postgres es necesario

Solo privilegios de INSERT

```
# su postgres
# psql

postgres=# \c aud_f2015-des
postgres=# GRANT INSERT ON ALL TABLES IN SCHEMA public TO auditoria; 
postgres=# GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO auditoria;
```
