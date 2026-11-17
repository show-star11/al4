# Big Data Practical 4: Schema Evolution with Kafka Schema Registry

**Name:** Arvind Yadav
**Seat no:** 31011123065

## Aim

Schema Evolution: Use Kafka Schema Registry to demonstrate a "Backward Compatible" schema change.

## Theory

**Schema.** A schema describes the structure of a message: the field names and their data types. This practical uses Avro schemas, written as JSON in `.avsc` files.

**Schema Registry.** Schema Registry is a service that runs next to Kafka and stores every version of every schema. Producers and consumers ask it for schemas through a REST API (here at `http://localhost:8081`), so both sides agree on the structure of the data.

**Subject.** Schemas are stored under a name called a subject, and each subject has its own list of versions. This practical uses the subject `patient-value`.

**Schema evolution.** Data changes over time: fields are added, removed or changed. Each change is registered as a new version of the schema. Schema Registry checks every new version against the compatibility rule of the subject and rejects it if it breaks the rule.

**Backward compatibility.** A new schema is backward compatible when a consumer using the new schema can still read data written with the old schema.

| Change | Backward compatible? | Reason |
| --- | --- | --- |
| Add a field with a default value | Yes | Old data does not have the field, so the default is used |
| Remove a field | Yes | The new reader ignores the extra field in old data |
| Add a field without a default | No | Old data has no value for it and there is nothing to fill in |
| Change a field's type, for example `int` to `string` | No | Old values cannot be read as the new type |

**What this practical shows.**

| Version | Change | Result |
| --- | --- | --- |
| V1 | First schema: `patient_id`, `patient_name`, `age`, `heart_rate` | Registered with id 1 |
| V2 | Adds `blood_pressure` with default `"Unknown"` | Accepted, registered with id 2 |
| V3 | Changes `patient_id` from `int` to `string` | Rejected with error 409 |

## Files

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Starts Kafka and Schema Registry |
| `patient-v1.avsc` | Schema V1 |
| `patient-v2.avsc` | Schema V2: adds `blood_pressure` with a default |
| `patient-v3.avsc` | Schema V3: changes the type of `patient_id` |
| `register-v1.json`, `register-v2.json`, `register-v3.json` | The schemas wrapped in the format the Schema Registry API accepts |
| `compatibility.json` | Sets the compatibility level to `BACKWARD` |
| `patient-v2.json` | A sample patient record that follows V2 |

## Requirements (Windows)

- Windows 10 or 11
- Docker Desktop, running
- VS Code
- `curl.exe` (included in Windows 10 and 11)

## Steps to run (Windows PowerShell)

**Step 1. Create the Kafka environment**

```powershell
git clone https://github.com/show-star/al4.git
cd al4
```

Open this folder in VS Code. Make sure Docker Desktop shows **Engine running**, then start the containers:

```powershell
docker compose up -d
```

**Step 2 and 3. Check that Kafka and Schema Registry are running**

```powershell
docker ps
```

Two containers should be listed with status **Up**: `kafka` (port 9092) and `schema-registry` (port 8081). Wait about 30 seconds before the next step so Schema Registry finishes starting.

**Step 4. Schema V1**

`patient-v1.avsc` contains the first schema, with four fields: `patient_id` (int), `patient_name` (string), `age` (int) and `heart_rate` (int).

**Step 5. Register schema V1**

`register-v1.json` holds the same schema as one text value under the key `"schema"`, which is the format the API needs.

```powershell
curl.exe -X POST "http://localhost:8081/subjects/patient-value/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data-binary "@register-v1.json"
```

Output: `{"id":1}`

**Step 6. Set BACKWARD compatibility**

```powershell
curl.exe -X PUT "http://localhost:8081/config/patient-value" -H "Content-Type: application/json" --data-binary "@compatibility.json"
```

The compatibility level of the subject is now `BACKWARD`.

**Step 7 and 8. Schema V2**

`patient-v2.avsc` is V1 plus one new field:

```json
{
  "name": "blood_pressure",
  "type": "string",
  "default": "Unknown"
}
```

**Step 9. Register schema V2**

```powershell
curl.exe -X POST "http://localhost:8081/subjects/patient-value/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data-binary "@register-v2.json"
```

Output: `{"id":2}`. The change is accepted because the new field has a default value.

**Step 10. Verify V2**

```powershell
curl.exe -X GET "http://localhost:8081/subjects/patient-value/versions"
```

Output: `[1,2]`

**Step 11. Send patient data**

Create the topic:

```powershell
docker exec kafka kafka-topics.sh --create --topic patient-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Start the producer:

```powershell
docker exec -it kafka kafka-console-producer.sh --topic patient-data --bootstrap-server localhost:9092
```

At the `>` prompt, paste the record from `patient-v2.json` on one line and press Enter:

```json
{"patient_id":101,"patient_name":"Amit","age":45,"heart_rate":78,"blood_pressure":"120/80"}
```

Press **Ctrl + C** to exit the producer.

**Step 12. Schema V3**

`patient-v3.avsc` is V2 with the type of `patient_id` changed from `int` to `string`.

**Step 13. Try to register schema V3**

```powershell
curl.exe -X POST "http://localhost:8081/subjects/patient-value/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data-binary "@register-v3.json"
```

**Step 14. Observe the rejection**

Schema Registry answers with `error_code: 409`, `TYPE_MISMATCH` and `reader type: STRING not compatible with writer type: INT`. V3 is rejected because `patient_id` is `int` in V2 and `string` in V3, so old data could not be read with the new schema.

**Step 15. Verify all schema versions**

```powershell
curl.exe -X GET "http://localhost:8081/subjects/patient-value/versions"
```

Output: `[1,2]`. V3 was not stored.

**Step 16. Stop the containers**

```powershell
docker compose down
```

This also deletes the registered schemas, because the containers do not use a volume. Running the steps again starts from V1.

## Output

```text
PS> curl.exe -X POST ".../subjects/patient-value/versions" ... "@register-v1.json"
{"id":1}

PS> curl.exe -X PUT ".../config/patient-value" ... "@compatibility.json"
{}

PS> curl.exe -X POST ".../subjects/patient-value/versions" ... "@register-v2.json"
{"id":2}

PS> curl.exe -X GET ".../subjects/patient-value/versions"
[1,2]

PS> docker exec kafka kafka-topics.sh --create --topic patient-data ...
Created topic patient-data.

PS> curl.exe -X POST ".../subjects/patient-value/versions" ... "@register-v3.json"
{"error_code":409,"message":"Schema being registered is incompatible with an earlier schema for subject \"patient-value\", details: [{errorType:'TYPE_MISMATCH', description:'The type (path '/fields/0/type') of a field in the new schema does not match with the old schema', additionalInfo:'reader type: STRING not compatible with writer type: INT'}, {oldSchemaVersion: 2}, ... {compatibility: 'BACKWARD'}]"}

PS> curl.exe -X GET ".../subjects/patient-value/versions"
[1,2]
```

## Observations

- V1 is registered with id 1 and V2 with id 2.
- V2 adds `blood_pressure` with a default value, so a reader using V2 can still read V1 data; the missing field becomes `"Unknown"`. This is a backward compatible change.
- V3 changes the type of `patient_id` from `int` to `string`. Schema Registry rejects it with error 409 (`TYPE_MISMATCH`), so incompatible changes cannot reach the consumers.
- The list of versions stays `[1,2]` after the failed V3 request.
- The console producer in step 11 sends the record as plain JSON text. It shows data in the V2 shape being sent to Kafka; the compatibility check itself is done by Schema Registry in steps 9 and 13.

## Errors on Windows and how to fix them

### Error 1: `{"error_code":42201,"message":"Empty schema"}`

**Cause:** the `.avsc` file was sent directly. The API does not accept a bare schema; it needs a JSON object with the schema as text under the key `"schema"`.

1. Use the `register-v1.json` file, not `patient-v1.avsc`:
   ```powershell
   curl.exe -X POST "http://localhost:8081/subjects/patient-value/versions" -H "Content-Type: application/vnd.schemaregistry.v1+json" --data-binary "@register-v1.json"
   ```
2. The output should be `{"id":1}`.

### Error 2: `{"error_code":400,"message":"Unexpected character ..."}` when setting compatibility

**Cause:** the JSON was typed directly in the command, for example `--data "{\"compatibilityLevel\":\"BACKWARD\"}"`. PowerShell removes or changes the quotes, so Schema Registry receives broken JSON.

1. Put the JSON in a file. `compatibility.json` already contains:
   ```json
   {
     "compatibilityLevel": "BACKWARD"
   }
   ```
2. Send the file with `--data-binary "@compatibility.json"`:
   ```powershell
   curl.exe -X PUT "http://localhost:8081/config/patient-value" -H "Content-Type: application/json" --data-binary "@compatibility.json"
   ```

### Error 3: `Invoke-WebRequest : A parameter cannot be found that matches parameter name 'X'`

**Cause:** in Windows PowerShell, `curl` is another name for `Invoke-WebRequest`, which does not understand these options.

1. Type `curl.exe` (with `.exe`) in every command.

### Error 4: `curl: (7) Failed to connect to localhost port 8081` or `curl: (52) Empty reply from server`

**Cause:** Schema Registry is not running or has not finished starting.

1. Run `docker ps` and check that `schema-registry` shows **Up**.
2. If it is missing, run `docker compose up -d`.
3. Wait 30 to 40 seconds and test with `curl.exe http://localhost:8081/subjects`. It is ready when this prints `[]`.
4. If it keeps stopping, see Error 9.

### Error 5: `curl: (26) Failed to open/read local data from file/application`

**Cause:** the terminal is not in the folder that contains the JSON files.

1. Run `dir` and check that `register-v1.json` is listed.
2. If not, go to the project folder with `cd`, using quotes when the path has spaces:
   ```powershell
   cd "C:\path\to\project folder"
   ```

### Error 6: `docker : The term 'docker' is not recognized`

**Cause:** Docker Desktop is not installed, or it is running but the terminal cannot find the `docker` program.

1. Open Docker Desktop and wait until it shows **Engine running**.
2. Run these lines one at a time. They find `docker.exe` and add it to the terminal's path:
   ```powershell
   $d = Split-Path (Get-Process "Docker Desktop" | Select-Object -First 1).Path
   $exe = Get-ChildItem $d, (Split-Path $d), "C:\Program Files\Docker", "$env:LOCALAPPDATA\Programs" -Filter docker.exe -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
   $env:PATH += ";" + $exe.DirectoryName
   docker --version
   ```
3. To keep the fix for every new terminal, run this once:
   ```powershell
   [Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";" + $exe.DirectoryName, "User")
   ```
4. If it still fails, click the **`>_`** icon at the bottom right of Docker Desktop and run the commands in that terminal after going to the project folder with `cd`.

### Error 7: `short read ... unexpected EOF` while downloading images

**Cause:** the internet connection dropped during the first download. The two images are about 900 MB together.

1. Run `docker compose up -d` again. Docker keeps the parts already downloaded and continues from there.
2. If it keeps failing, download the images one at a time, then start:
   ```powershell
   docker pull bitnamilegacy/kafka:3.7
   docker pull confluentinc/cp-schema-registry:7.6.1
   docker compose up -d
   ```

### Error 8: `Bind for 0.0.0.0:8081 failed: port is already allocated`

**Cause:** another container already uses port 8081 (or 9092).

1. Find it:
   ```powershell
   docker ps -a --filter "publish=8081"
   ```
2. If it belongs to another project you still need, stop it instead of deleting it:
   ```powershell
   docker stop <container-name>
   ```
3. Start this practical with `docker compose down`, then `docker compose up -d`.
4. When finished, run `docker compose down` and start the other container again with `docker start <container-name>`.

If the Kafka cluster from Practical 3 (`kafka1`, `kafka2`, `kafka3`) is still running, stop it too with `docker stop kafka1 kafka2 kafka3`, so it does not use memory.

### Error 9: Schema Registry stops: `Exited (1)` and `DNS resolution failed for kafka`

**Cause:** the `schema-registry` container was created during an earlier failed start (for example while port 8081 was taken) and is not connected to Kafka's network, so it cannot find the container named `kafka`. Restarting it keeps the broken setup.

1. Check its status and log:
   ```powershell
   docker ps -a --filter "name=schema-registry"
   docker logs schema-registry --tail 30
   ```
2. If the status is `Exited` and the log shows `DNS resolution failed for kafka` or `No resolvable bootstrap urls`, remove and recreate both containers:
   ```powershell
   docker compose down
   docker compose up -d
   ```
3. Wait 40 seconds and test:
   ```powershell
   curl.exe http://localhost:8081/subjects
   ```
   Schema Registry is ready when this prints `[]`.
4. If it shows `Exited` again with a different message, it started before Kafka was ready. Run `docker compose restart schema-registry` and wait 40 seconds.

## Conclusion

Kafka and Schema Registry were started with Docker. Schema V1 was registered for the subject `patient-value` and the compatibility level was set to `BACKWARD`. V2, which adds a field with a default value, was accepted as a backward compatible change, while V3, which changes the type of a field, was rejected. This shows how Schema Registry lets a schema evolve safely without breaking consumers that read older data.
