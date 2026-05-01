'use strict'
const { DataApiClient } = require('rqlite-js')
const log = require('./logger')

const CACHE_HOSTS = process.env.MEDIA_CACHE_URL || ['http://bot-cache-0.bot-cache-internal.datastore.svc.cluster.local:4001', 'http://bot-cache-1.bot-cache-internal.datastore.svc.cluster.local:4001', 'http://bot-cache-2.bot-cache-internal.datastore.svc.cluster.local:4001']
const NAME_SPACE = process.env.CACHE_NAMESPACE || process.env.NAME_SPACE || 'default'

const dataApiClient = new DataApiClient(CACHE_HOSTS)
let CACHE_READY = false

const CACHE_TABLES = { yt: {}, tiktok: {}}

async function createYTTable(){
  try{
    CACHE_TABLES.yt.table = `yt_media_feeds_${NAME_SPACE}`
    let sql = `CREATE TABLE IF NOT EXISTS "${CACHE_TABLES.yt.table}" (id TEXT PRIMARY KEY, ytChId TEXT NOT NULL, username TEXT NOT NULL, title TEXT NOT NULL, share_url TEXT NOT NULL, create_time INTEGER NOT NULL, video_description TEXT, chId TEXT, msgId TEXT, ttl INTEGER NOT NULL)`
    CACHE_TABLES.yt.all = `SELECT * FROM "${CACHE_TABLES.yt.table}" WHERE ytChId=:ytChId`
    CACHE_TABLES.yt.set = `INSERT OR REPLACE INTO "${CACHE_TABLES.yt.table}" (id, ytChId, username, title, share_url, create_time, video_description, chId, msgId, ttl) VALUES(:id, :ytChId, :username, :title, :share_url, :create_time, :video_description, :chId, :msgId, :ttl)`
    let dataResults = await dataApiClient.execute(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      setTimeout(init, 5000)
      return
    }
    log.info(`created rqlite table ${CACHE_TABLES.yt.table}`)
    return true
  }catch(e){
    log.error(e)
  }
}
async function createTikTokTable(){
  try{
    CACHE_TABLES.tiktok.table = `tiktok_media_feeds_${NAME_SPACE}`
    let sql = `CREATE TABLE IF NOT EXISTS "${CACHE_TABLES.tiktok.table}" (id TEXT PRIMARY KEY, open_id TEXT NOT NULL, username TEXT NOT NULL, title TEXT NOT NULL, share_url TEXT NOT NULL, create_time INTEGER NOT NULL, video_description TEXT, like_count INTEGER, view_count INTEGER, chId TEXT, msgId TEXT, ttl INTEGER NOT NULL)`
    CACHE_TABLES.tiktok.all = `SELECT * FROM "${CACHE_TABLES.tiktok.table}" WHERE open_id=:open_id`
    CACHE_TABLES.tiktok.set = `INSERT OR REPLACE INTO "${CACHE_TABLES.tiktok.table}" (id, open_id, username, title, share_url, create_time, video_description, like_count, view_count, chId, msgId, ttl) VALUES(:id, :open_id, :username, :title, :share_url, :create_time, :video_description, :like_count, :view_count, :chId, :msgId, :ttl)`
    let dataResults = await dataApiClient.execute(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      setTimeout(init, 5000)
      return
    }
    log.info(`created rqlite table ${CACHE_TABLES.tiktok.table}`)
    return true
  }catch(e){
    log.error(e)
  }
}
const init = async()=>{
  try{
    let status = await createYTTable()
    if(status) status = await createTikTokTable()
    if(!status){
      log.debug(`Error creating media_feeds tables`)
      setTimeout(init, 5000)
      return
    }
    CACHE_READY = true
  }catch(e){
    log.error(e)
    setTimeout(init, 5000)
  }
}
init()
async function getCache(key, table){
  try{
    if(!key || !table || !CACHE_READY || !CACHE_TABLES[table]?.table) return

    let sql = `SELECT * FROM "${CACHE_TABLES[table].table}" WHERE id="${key.toString()}"`
    let res = await dataApiClient.query(sql)
    if(res.hasError()){
      log.error(res?.getFirstError())
      return
    }
    return res.get(0)
  }catch(e){
    log.error(e)
  }
}
async function setCache(val, table){
  try{
    if(!val  || !table || !CACHE_READY || !CACHE_TABLES[table]?.set) return

    let sql = [
      [CACHE_TABLES[table].set, {...val,ttl: Date.now()}]
    ]
    let dataResults = await dataApiClient.execute(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      return
    }
    return dataResults?.get(0)?.getRowsAffected()
  }catch(e){
    log.error(e)
  }
}
async function delCache(key, table){
  try{
    if(!key || !table || !CACHE_READY || CACHE_TABLES[table].table) return

    let sql = `DELETE FROM "${CACHE_TABLES[table].table}" WHERE id="${key}"`
    let dataResults = await dataApiClient.execute(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      return
    }
    return dataResults?.get(0)?.getRowsAffected()
  }catch(e){
    log.error(e)
  }
}
async function getAll (key, table){
  try{

    if(!key  || !table || !CACHE_READY || !CACHE_TABLES[table]?.all) return
    let sql = [
      [CACHE_TABLES[table]?.all, key]
    ]
    let dataResults = await dataApiClient.query(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      return
    }
    return dataResults?.toArray()
  }catch(e){
    log.error(e)
  }
}
async function update (id, key, value, table){
  try{
    if(!key  || !table || !CACHE_READY || !CACHE_TABLES[table]?.table) return
    let sql = `UPDATE INTO "${CACHE_TABLES[table].table}" SET ${key}="${value}" WHERE id=${id}`
    let dataResults = await dataApiClient.execute(sql)
    if(dataResults?.hasError()){
      log.error(dataResults?.getFirstError())
      return
    }
    return dataResults?.toArray()
  }catch(e){
    log.error(e)
  }
}
module.exports = {
  del: delCache,
  all: getAll,
  status: ()=> { return CACHE_READY },
  set: setCache,
  get: getCache,
  update
}
