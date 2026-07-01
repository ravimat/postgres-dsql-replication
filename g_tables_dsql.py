# Save the DDLs to a file on EC2, then run:
sudo bash -c 'set -a && source /opt/cdc/.env && set +a && python3.11 -c "
import boto3, psycopg2, os

host = os.environ[\"DSQL_HOSTNAME\"]
region = os.environ[\"DSQL_REGION\"]
client = boto3.client(\"dsql\", region_name=region)
token = client.generate_db_connect_admin_auth_token(Hostname=host, Region=region, ExpiresIn=900)

ddls = open(\"/tmp/dsql_ddls.sql\").read().split(\";\")

for ddl in ddls:
    ddl = ddl.strip()
    if not ddl or ddl.startswith(\"--\"):
        continue
    try:
        conn = psycopg2.connect(host=host, port=5432, dbname=\"postgres\", user=\"admin\", sslmode=\"require\", **{\"password\": token})
        conn.autocommit = True
        conn.cursor().execute(ddl)
        conn.close()
        name = ddl.split(\"(\")[0].strip().replace(\"CREATE TABLE IF NOT EXISTS \", \"\")
        print(f\"OK: {name}\")
    except Exception as e:
        print(f\"ERR: {str(e)[:80]}\")
"'
