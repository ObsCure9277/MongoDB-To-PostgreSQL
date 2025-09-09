// import mongoose from 'mongoose';

// /**
//    * Get data from source collection
//    * @param {string} mongooseConn - Connection to mongoose
//    * @param {string} collectionName - Collection name
//    * @return {Array} Retrieved objects
//    */
// export default async (mongooseConn, collectionName) => {
//   const Model = mongooseConn.model(collectionName,
//     new mongoose.Schema({}, { collection: collectionName })
//   );
//   const result = await Model.find({});
//   return result.map((r) => {
//     delete r._doc.__v;
//     return r._doc;
//   });
// };



//new
// get-from-mongo.js
// Reads documents from Mongo using lean() for speed,
// returns plain objects, adds mongo_id, strips __v.

import mongoose from "mongoose";

/**
 * Connect to MongoDB, fetch collections, return plain docs.
 * @param {Object} cfg
 * @param {string} cfg.uri - Mongo connection string
 * @param {string[]} cfg.collections - list of collection names to read
 * @returns {Promise<Object>} { data, counts }
 */
async function getFromMongo({ uri, collections }) {
  if (!uri) throw new Error("Mongo URI missing");

  const conn = await mongoose.createConnection(uri, {
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 20000,
  }).asPromise();

  // Generic model factory (no schema: strict: false)
  const getModel = (name) =>
    conn.model(
      name,
      new mongoose.Schema({}, { strict: false, versionKey: false }),
      name
    );

  const data = {};
  const counts = {};

  for (const col of collections) {
    try {
      const Model = getModel(col);
      const docs = await Model.find({}, null, { lean: true });
      const cleaned = docs.map((d) => {
        const o = { ...d };
        // preserve _id as string to keep stable mapping
        if (o._id != null) {
          o.mongo_id = String(o._id);
          delete o._id;
        }
        // strip Mongoose internals just in case
        delete o.__v;
        return o;
      });
      data[col] = cleaned;
      counts[col] = cleaned.length;
      console.log(`[mongo] ${col}: fetched ${cleaned.length} docs`);
    } catch (err) {
      console.warn(`[mongo] WARN reading ${col}: ${err.message}`);
      data[col] = [];
      counts[col] = 0;
    }
  }

  await conn.close();
  return { data, counts };
}

export default getFromMongo;
