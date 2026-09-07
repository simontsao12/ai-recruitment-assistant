# Liquibase XML changeset 轉換

本次將原本 sqlFile 引用的 V1／V2 SQL 改為同一主 changelog 內的 XML change types。
兩個 changeset 的 ID、author、檔案位置不變，資料表與欄位的目的及結構保持一致。

- 主檔：src/main/resources/db/changelog/db.changelog-master.xml
- 001-initial-schema / recruitment
- 002-pipeline / recruitment

createTable、addColumn、createIndex、addPrimaryKey 表達一般 schema。
PostgreSQL 分數 CHECK 與 partial expression unique index 以 changeset 內 sql 保留原語意。
原 src/main/resources/db/migration 下的兩個 SQL 已移除。

## 新資料庫

可由應用啟動時的 Liquibase 直接初始化。

## 已執行舊版 Liquibase 的資料庫

SQL 轉 XML 會改變 checksum。啟動前請：

1. 停止應用並確認資料庫備份。
2. 確認實際 schema 對應已執行的舊 V1／V2 內容。
3. 查閱原 changeset 身分與執行結果：

```sql
SELECT id, author, filename, md5sum, exectype
FROM databasechangelog
WHERE author = 'recruitment'
  AND id IN ('001-initial-schema', '002-pipeline');
```

預期 filename 為 db/changelog/db.changelog-master.xml。若不同，先釐清當時載入路徑；
不要藉由重新建立 changeset 紀錄略過差異。

4. 只有在確認身分與 schema 相符後，將這兩筆已存在紀錄的 checksum 清空：

```sql
UPDATE databasechangelog
SET md5sum = NULL
WHERE author = 'recruitment'
  AND id IN ('001-initial-schema', '002-pipeline')
  AND filename = 'db/changelog/db.changelog-master.xml';
```

更新筆數應等於第 3 步中符合路徑的已執行紀錄數。下一次 Liquibase 啟動會重新計算
這些已執行 changeset 的 checksum；仍未執行的 changeset 會正常執行。清空 checksum 不會驗證或修復實際 schema 差異，因此不能省略前面的結構核對。

此 SQL 只供人工核對後執行，本次程式修改不會自動執行它。
不要刪除這兩筆執行紀錄或整張 DATABASECHANGELOG、不要使用 runOnChange 重跑建表，也不要把所有 changeset 直接標為已執行。

如果資料庫只有 Flyway 歷史，需另外依實際 schema 做 Liquibase baseline，以上 checksum 更新不適用。

## 設計依據

[Liquibase changeset checksum 說明](https://docs.liquibase.com/community/user-guide-5-0-4/what-is-a-changeset-checksum)：
內容變更需處理 checksum；可只將對應 id／author／filepath 紀錄的 checksum 設為 null，
避免清除整個資料庫的 checksum。
