var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var Busboy = require('busboy');
//var fileUpload = require('express-fileupload');
var cassandra = require('cassandra-driver');
var client = new cassandra.Client({ contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace: 'hw5' });

client.connect();
//client.execute('select key from system.local', function (err, result) {
  //if(err) throw err;
  //console.log(result.rows[0]);
//});

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
//app.use(fileUpload());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

app.post('/deposit', function(req, res, next) {
  var busboy = new Busboy({ headers: req.headers });
  var name;
  var contents = new Buffer(0);
  busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
      //console.log('File [' + fieldname + ']: filename: ' + filename + ', encoding: ' + encoding + ', mimetype: ' + mimetype);
      name = filename;
      file.on('data', function(data) {
        //console.log('File [' + fieldname + '] got ' + data.length + ' bytes');
        contents = Buffer.concat([contents, data]);
      });
      file.on('end', function() {
        console.log('File [' + fieldname + '] Finished');
      });
    });
    busboy.on('field', function(fieldname, val, fieldnameTruncated, valTruncated, encoding, mimetype) {
      //console.log('Field [' + fieldname + ']: value: ' + inspect(val));
    });
    busboy.on('finish', function() {
      console.log('Done parsing form!');
      client.execute('INSERT INTO imgs (filename, contents) VALUES (?, ?)', [name, contents], function(err){
                        if(err){
                                console.log(err);
                        } else {
                                console.log("inserted successfully");
                        }
      });
      res.writeHead(200, { Connection: 'close' });
      res.end();
    });
    req.pipe(busboy);
});

app.get('/retrieve', function (req, res, next) {
  var filename = req.query.filename;
  var contents = '';
	client.execute('SELECT contents FROM imgs WHERE filename = ?', [filename], function(err, result){
		if(err){
			console.log("Error on retrieve data from cassandra");
		} else {
			contents = result.rows[0].contents;
			console.log(contents.length);
			var type = filename.substr(filename.length - 3);
			res.writeHead(200, { 'Content-Type': 'image/' + type });
			res.write(contents);
			res.end();
		}
	});
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
