'use strict'
function getTimeStamp(timestamp){
  if(!timestamp) timestamp = Date.now()
  let dateTime = new Date(timestamp)
  return dateTime.toLocaleString('en-US', { timeZone: 'Etc/GMT+5', hour12: false })
}
module.exports.error = (err)=>{
  console.error(`${getTimeStamp(Date.now())} ERROR [reactions-cache] ${err}`)
}
module.exports.info = (msg)=>{
  console.log(`${getTimeStamp(Date.now())} INFO [reactions-cache] ${msg}`)
}
