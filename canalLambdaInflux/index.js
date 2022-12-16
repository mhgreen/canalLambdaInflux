const { InfluxDB, Point, HttpError } = require('@influxdata/influxdb-client');

//This code writes data from IoT core rule via Lambda into InfluxDB 

exports.handler = async (event, context, callback) => {
  console.log('*** event ***');
  console.log(event);

  const sensorIdInputValue = event.dev_eui;
  const distanceInputValue = event.distance;
  const tempcInputValue = event.caseTempC;
  const utcInputValue = event.utc;
  const vbatInputValue = event.vBat;
  const wtempCInputValue = event.waterTempC;

  let result = writeToInfluxDB(distanceInputValue, tempcInputValue, utcInputValue, vbatInputValue, sensorIdInputValue, wtempCInputValue);

  callback(null, result);

};

function writeToInfluxDB(distanceInputValue, tempcInputValue, utcInputValue, vbatInputValue, sensorIdInputValue, wtempCInputValue) {
  console.log("Executing Iflux insert");

  /** InfluxDB v2 URL */
  const url = process.env.url;
  /** InfluxDB authorization token */
  const token = process.env.token;
  /** Organization within InfluxDB  */
  const org = process.env.org;
  /** Bucket within InfluxDB  */
  const bucket = process.env.bucket;

  console.log('*** WRITE POINTS ***');
  // create a write API, expecting point timestamps in nanoseconds (default 'ns', can be also 's', 'ms', 'us')
  const writeApi = new InfluxDB({ url, token }).getWriteApi(org, bucket, 's');
  const observationPoint = new Point('dockSensor')
    .tag('sensorID', sensorIdInputValue)
    .uintField('distance', distanceInputValue)
    .floatField('caseTempC', tempcInputValue)
    .floatField('waterTempC', wtempCInputValue)
    .floatField('vbat', vbatInputValue)
    .uintField('timeStamp', utcInputValue)
    .timestamp(utcInputValue);
  writeApi.writePoint(observationPoint);
  console.log(` ${observationPoint.toLineProtocol(writeApi)}`);

  // WriteApi always buffer data into batches to optimize data transfer to InfluxDB server.
  // writeApi.flush() can be called to flush the buffered data. The data is always written
  // asynchronously, Moreover, a failed write (caused by a temporary networking or server failure)
  // is retried automatically. Read `writeAdvanced.js` for better explanation and details.
  //
  // close() flushes the remaining buffered data and then cancels pending retries.
  writeApi
    .close()
    .then(() => {
      console.log('FINISHED ...');
    })
    .catch(e => {
      console.error(e);
      if (e instanceof HttpError && e.statusCode === 401) {
        console.log('HTTP Error 401');
      }
      console.log('\nFinished ERROR');
    });
}
