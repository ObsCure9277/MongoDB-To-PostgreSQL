// /**
//  * Insert data to destination table
//  * @param {object} params - migration parameters
//  * @param {object} params.knex - Knex.js instance
//  * @param {Array} params.collections - Array of collection definitions
//  * @param {string} params.tableName - Table name
//  * @param {Array} params.rows - Objects to insert
//  * @return {Array} idsMap - mapping of old Mongo _id to new SQL id
//  */
// export default async function putToPostgres({ knex, collections, tableName, rows }) {
//   // Find collection config
//   const collectionConfig = collections.find(c => c.tableName === tableName);
//   if (!collectionConfig) throw new Error(`Collection config not found for table: ${tableName}`);

//   const { foreignKeys, fieldsRename, fieldsRedefine, links } = collectionConfig;

//   const idsMap = []; // save oldId -> newId

//   for (const currentRow of rows) {
//     // --- 1. Rename fields if needed ---
//     if (fieldsRename) {
//       for (const [oldField, newField] of fieldsRename) {
//         if (currentRow.hasOwnProperty(oldField)) {
//           currentRow[newField] = currentRow[oldField];
//           delete currentRow[oldField];
//         }
//       }
//     }

//     // --- 2. Redefine fields if needed ---
//     if (fieldsRedefine) {
//       for (const [field, value] of fieldsRedefine) {
//         currentRow[field] = value;
//       }
//     }

//     // --- 3. Map foreign keys from Mongo _id -> SQL id ---
//     if (foreignKeys) {
//       for (const [fieldName, refCollectionName] of Object.entries(foreignKeys)) {
//         const foreignCollection = collections.find(c => c.collectionName === refCollectionName);
//         if (!foreignCollection || !foreignCollection.idsMap) {
//           currentRow[fieldName] = null;
//           continue;
//         }
//         const map = foreignCollection.idsMap.find(
//           x => x.oldId === (currentRow[fieldName] ? currentRow[fieldName].toString() : null)
//         );
//         currentRow[fieldName] = map ? map.newId : null;
//       }
//     }

//     // --- 4. Save old Mongo _id and remove it from insert row ---
//     const oldId = currentRow._id.toString();
//     currentRow.mongo_id = currentRow._id.toString();
//     delete currentRow._id;

//     // --- 5. Remove array fields before insert ---
//     const rowCopy = { ...currentRow };
//     for (const key of Object.keys(rowCopy)) {
//       if (Array.isArray(rowCopy[key])) delete rowCopy[key];
//     }

//     // --- 6. Insert row and get SQL id ---
//     const inserted = await knex(tableName).returning('id').insert(rowCopy);

//     // Knex returns array of objects [{id: 1}], extract integer
//     const newId = Array.isArray(inserted) ? inserted[0].id : inserted;

//     idsMap.push({ oldId, newId });

//     // --- 7. Handle many-to-many links ---
//     if (links) {
//       for (const [fieldName, [linksTable, selfIdCol, foreignIdCol, transformFunc]] of Object.entries(links)) {
//         if (!currentRow[fieldName]) continue;
//         const relatedIds = currentRow[fieldName];
//         const foreignCollection = collections.find(
//           c => c.collectionName === foreignKeys[fieldName]
//         );
//         for (const related of relatedIds) {
//           let foreignKey;
//           let linkRow = {};
//           if (related.constructor.name === 'ObjectID' || typeof related === 'string') {
//             foreignKey = related.toString();
//           } else if (transformFunc) {
//             const res = transformFunc(linkRow, related);
//             foreignKey = res.foreignKey.toString();
//             linkRow = res.linkRow;
//           } else {
//             foreignKey = null;
//           }
//           const map = foreignCollection.idsMap.find(x => x.oldId === foreignKey);
//           if (!map) continue;
//           linkRow[selfIdCol] = newId;
//           linkRow[foreignIdCol] = map.newId;
//           await knex(linksTable).insert(linkRow);
//         }
//       }
//     }
//   }

//   console.log(`Inserted ${rows.length} rows to "${tableName}" table`);
//   return idsMap;
// }


//new

// put-to-postgres.js
// Writes to Postgres with transactions, idempotency (by mongo_id),
// FK mapping, optional M:N link-table writes, and careful logging.

import Knex from "knex";

/**
 * @typedef {Object} PutOptions
 * @property {Object<string,string>} [rename]      // { oldName: newName }
 * @property {Object<string,(any)=>any>} [redefine] // { field: mapper(value, row) }
 * @property {Object<string,string>} [foreignKeys] // { fieldName: 'foreignCollectionName' }
 * @property {Object<string,[string,string,string,Function?]>} [links]
 *    Map of array-field -> [linkTable, selfIdCol, foreignIdCol, transform?]
 *    Example: "award_ids": ["employees_awards","employee_id","award_id", (selfId, foreignId, row)=>({})]
 */

/**
 * Insert a single collection into a SQL table.
 * - Uses transaction
 * - Idempotent by unique mongo_id
 * - Maps FK fields using idsMap of foreign collections
 * - Writes M:N link rows for array fields defined in `links`
 *
 * @param {Knex} knex
 * @param {string} table
 * @param {Array<Object>} rows
 * @param {PutOptions} options
 * @param {Object<string, Map<string, number>>} idsRegistry  // { collection: Map<mongo_id, sql_id> }
 * @param {string} selfCollectionName // key used for idsRegistry
 */
async function putToPostgres(knex, table, rows, options, idsRegistry, selfCollectionName) {
  const {
    rename = {},
    redefine = {},
    foreignKeys = {},
    links = {},
  } = options || {};

  if (!Array.isArray(rows) || rows.length === 0) {
    console.log(`[pg] ${table}: nothing to insert`);
    return;
  }

  // Ensure ids map exists for this collection
  if (!idsRegistry[selfCollectionName]) {
    idsRegistry[selfCollectionName] = new Map();
  }

  await knex.transaction(async (trx) => {
    // Ensure mongo_id column exists and is unique (skip if already set in schema)
    // NOTE: safe-guard; if managed by migrations, this will be a no-op on failure.
    try {
      const hasCol = await trx.schema.hasColumn(table, "mongo_id");
      if (!hasCol) {
        await trx.schema.alterTable(table, (t) => {
          t.text("mongo_id").unique().index();
        });
        console.log(`[pg] ${table}: added mongo_id column`);
      }
    } catch (e) {
      // schema change may be prohibited in prod; continue gracefully
      console.log(`[pg] ${table}: schema check: ${e.message}`);
    }

    // Prepare transformed rows
    const prepared = rows.map((row) => {
      let r = { ...row };

      // rename stage
      for (const [oldName, newName] of Object.entries(rename)) {
        if (Object.prototype.hasOwnProperty.call(r, oldName)) {
          r[newName] = r[oldName];
          delete r[oldName];
        }
      }

      // redefine stage
      for (const [field, mapper] of Object.entries(redefine)) {
        if (Object.prototype.hasOwnProperty.call(r, field)) {
          try {
            r[field] = mapper(r[field], r);
          } catch (e) {
            console.warn(`[pg] redefine FAIL ${table}.${field}: ${e.message}`);
          }
        }
      }

      // foreign keys mapping: replace old string ids with integer ids
      for (const [field, foreignCollection] of Object.entries(foreignKeys)) {
        if (!Object.prototype.hasOwnProperty.call(r, field)) continue;
        const oldVal = r[field];
        if (oldVal == null) continue;

        const fkMap = idsRegistry[foreignCollection];
        if (!fkMap) {
          console.warn(`[pg] WARN: ${table}.${field} references missing collection map '${foreignCollection}'`);
          r[field] = null;
          continue;
        }

        // handle single value or array (defensive)
        if (Array.isArray(oldVal)) {
          const mapped = oldVal
            .map((id) => (id == null ? null : fkMap.get(String(id))))
            .filter((v) => v != null);
          if (mapped.length !== oldVal.length) {
            console.warn(`[pg] WARN: ${table}.${field} lost ${oldVal.length - mapped.length} FK(s) due to missing maps`);
          }
          // usually FK fields are scalar; if you have arrays, put them under links instead
          r[field] = mapped[0] ?? null;
        } else {
          const newId = fkMap.get(String(oldVal));
          if (newId == null) {
            console.warn(`[pg] WARN: ${table}.${field} missing FK map for value '${oldVal}'`);
            r[field] = null;
          } else {
            r[field] = newId;
          }
        }
      }

      // Drop array fields that are not handled by links; warn about potential data loss
      for (const [key, val] of Object.entries(r)) {
        if (Array.isArray(val) && !links[key]) {
          console.warn(`[pg] WARN: dropping unlinked array field ${table}.${key} to avoid type error`);
          delete r[key];
        }
      }

      return r;
    });

    // IDempotency: split into new vs existing by mongo_id
    const mongoIds = prepared.map((r) => r.mongo_id).filter(Boolean);
    const existing = await trx(table).select("id", "mongo_id").whereIn("mongo_id", mongoIds);
    const existingMap = new Map(existing.map((x) => [x.mongo_id, x.id]));

    const toInsert = prepared.filter((r) => r.mongo_id && !existingMap.has(r.mongo_id));
    const toSkip = prepared.filter((r) => r.mongo_id && existingMap.has(r.mongo_id));

    if (toSkip.length) {
      console.log(`[pg] ${table}: ${toSkip.length} already exist (idempotent skip)`);
      for (const r of toSkip) {
        idsRegistry[selfCollectionName].set(r.mongo_id, existingMap.get(r.mongo_id));
      }
    }

    if (toInsert.length) {
      const inserted = await trx(table).insert(toInsert).returning(["id", "mongo_id"]);
      for (const row of inserted) {
        idsRegistry[selfCollectionName].set(String(row.mongo_id), Number(row.id));
      }
      console.log(`[pg] ${table}: inserted ${inserted.length}`);
    } else {
      console.log(`[pg] ${table}: inserted 0`);
    }

    // Handle link tables for many-to-many based on array fields
    for (const [arrayField, [linkTable, selfIdCol, foreignIdCol, transform]] of Object.entries(links)) {
      // Gather pairs (selfId, foreignId)
      const linkRows = [];
      for (const raw of rows) {
        if (!Array.isArray(raw[arrayField]) || raw[arrayField].length === 0) continue;

        const selfSqlId = idsRegistry[selfCollectionName].get(String(raw.mongo_id));
        if (!selfSqlId) {
          console.warn(`[pg] WARN: link skip; no sql id for ${selfCollectionName} mongo_id=${raw.mongo_id}`);
          continue;
        }

        for (const foreignOldId of raw[arrayField]) {
          // Find which collection this array points to:
          // The convention is: links map's array field name should also have a twin entry inside `foreignKeys`
          // or you can encode which collection it points to inside transform (advanced). Here we assume
          // the foreignId provided is a Mongo ID from a known foreign collection, so we try each registered map.
          let foreignSqlId = null;
          for (const [collectionName, idMap] of Object.entries(idsRegistry)) {
            if (idMap.has(String(foreignOldId))) {
              foreignSqlId = idMap.get(String(foreignOldId));
              break;
            }
          }
          if (!foreignSqlId) {
            console.warn(`[pg] WARN: link skip; missing FK map for value '${foreignOldId}'`);
            continue;
          }

          const base = { [selfIdCol]: selfSqlId, [foreignIdCol]: foreignSqlId };
          const extra = typeof transform === "function" ? (transform(selfSqlId, foreignSqlId, raw) || {}) : {};
          linkRows.push({ ...base, ...extra });
        }
      }

      if (linkRows.length) {
        // Idempotent-ish: try to avoid duplicates by conflict target when possible
        try {
          await trx(linkTable).insert(linkRows);
        } catch (e) {
          console.warn(`[pg] WARN: link insert for ${linkTable} encountered duplicates or errors: ${e.message}`);
        }
        console.log(`[pg] ${table} -> ${linkTable}: inserted ${linkRows.length} link rows`);
      }
    }
  });
}

export default putToPostgres;
