const fs = require('fs')
const dotenv = require('dotenv')
const moment = require('moment')
const request = require('request')
const mysql = require('mysql')
const parallel = require('node-parallel')
const FTP = require('jsftp')

// Configuration
dotenv.config() 
var debugMode = !! process.env['DEBUG']

var stop = moment(process.argv[2] || Date.now())
var range = parseInt(process.argv[3]) || (0.5 * 60 * 60)

if ( ! stop.isValid()) {
	console.error('Invalid end date given!')
	process.exit(1)
}

if ( ! range) {
	console.error('Invalid range given!')
	process.exit(1)
}

var start = stop.clone().subtract(range, 'seconds')

console.log('Uploading data from %s to %s..', start, stop)

// Fetch devices
request({
	url: process.env['CONCAVA_API_HOST'] + '/devices',
	qs: {
		include: 'labels',
	},
	headers: {
		'Authorization': 'Token ' + process.env['CONCAVA_API_TOKEN'],
		'Accept': 'application/json',
	},
	json: true,
}, (err, res, devices) => {
	if (err) {
		console.error('Error fetching devices:', err)
		process.exit(1)
	}

	// Build metadata
	var metadata = buildMetadata(devices)

	// Build data
	var client = mysql.createConnection({
		host: process.env['MYSQL_HOST'],
		user: process.env['MYSQL_USER'],
		password: process.env['MYSQL_PASSWORD'],
		database: process.env['MYSQL_DATABASE'],
	})

	var results = {}
	var p = parallel().timeout(2 * 60 * 1000)

	devices.forEach((device) => {
		p.add((done) => {
			console.log('Fetching measurements for %s..', device.udid)

			client.query(
				`
					SELECT *, UNIX_TIMESTAMP(timestamp) as timestamp
					FROM ??
					WHERE timestamp >= FROM_UNIXTIME(?) AND timestamp <= FROM_UNIXTIME(?)
					ORDER BY timestamp DESC
				`,
				[
					device.udid,
					start.format('X'),
					stop.format('X'),
				],
				(err, rows) => {
					if (err) return done(err)

					results[device.udid] = rows
					done()
				}
			)
		})
	})

	p.done((err) => {
		client.end()

		if (err) {
			console.error('Error fetching data:', err)
			process.exit(1)
		}

		var data = buildData(results)
		var timestamp = stop.format('X')

		// Log data
		fs.writeFileSync('/tmp/metadata.' + timestamp + '.tsv', metadata, { encoding: 'ASCII' })
		fs.writeFileSync('/tmp/data.' + timestamp + '.csv', data, { encoding: 'ASCII' })

		// Upload (meta)data over FTP
		var ftp = new FTP({
			host: process.env['FTP_HOST'],
			port: 21,
			user: process.env['FTP_USER'],
			pass: process.env['FTP_PASSWORD'],
			debugMode,
		})

		ftp.on('jsftp_debug', (type, ev) => {
			console.info('FTP debug', type, ev)
		})

		ftp.put(metadata, 'metadata.txt', (err) => {
			if (err) {
				console.error('Error uploading metadata:', err)
				process.exit(1)
			}

			ftp.put(data, 'data' + timestamp + '.txt', (err) => {
				if (err) {
					console.error('Error uploading data:', err)
					process.exit(1)
				}

				console.log('Done.')
				process.exit(0)
			})
		})
	})
})

function buildMetadata (devices) {
	var data = 'ID\tlon\tlat\taltitude_m\tlocal_time\tcountry\tname\n'

	if (debugMode) console.log('Devices:', devices)

	devices.forEach((device) => {
		var labels = getLabels(device)

		data += `${device.udid}\t${labels.longitude}\t${labels.latitude}\t` +
			`${labels.altitude}\t${labels.timezone}\t${labels.country}\t${device.name}\n`
	})

	data = data.replace(/null/g, '').replace(/undefined/g, '')

	return new Buffer(data, 'ASCII')
}

function getLabels (device) {
	var labels = {}

	device.labels.forEach((label) => {
		labels[label.key] = label.value
	})

	return labels
}

function buildData (results) {
	var data = 'ID,Epoch_Time,mm rain,windspeed_kmh,gust_kmh,wind_dir,gust_dir,solar,temp,hum,pres\n'

	Object.keys(results).forEach((udid) => {
		results[udid].forEach((row) => {
			var solarRad = (row.solarRad !== undefined ? row.solarRad : row.maxSolar1)

			data += `${udid},${row.timestamp},${row.rain},${row.windSpeed},${row.gustSpeed},${row.windDir},` +
				`${row.gustDir},${solarRad},${row.temp},${row.humid},${row.pressure}\n`
		})
	})

	data = data.replace(/null/g, '').replace(/undefined/g, '')

	return new Buffer(data, 'ASCII')
}
