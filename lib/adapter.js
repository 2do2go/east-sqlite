'use strict';

var sqlite3 = require('sqlite3'),
	path = require('path');

var Adapter = function(params) {
	this.params = params || {};
	if (!this.params.url) {
		throw new Error('Db file should be set');
	}
};

Adapter.prototype.getTemplatePath = function() {
	return path.join(__dirname, 'migrationTemplate.js');
};

Adapter.prototype.connect = function(callback) {
	var self = this;
	var db = new sqlite3.Database(this.params.url, function(err) {
		if (err) return callback(err);
		self.db = db;
		// create (or not) _mingrations table
		db.run(
			'create table if not exists _migrations (' +
				'name text primary key' +
			')',
			function(err) {
			if (err) return callback(err);
			// return db object
			callback(null, {db: db});
		});
	});
};

Adapter.prototype.disconnect = function(callback) {
	if (callback) callback(null);
};

Adapter.prototype.getExecutedMigrationNames = function(callback) {
	this.db.all('select name from _migrations', function(err, migrations) {
		if (err) return callback(err);
		callback(null, migrations.map(function(migration) {
			return migration.name;
		}));
	});
};

Adapter.prototype.markExecuted = function(name, callback) {
	this.db.run('insert or replace into _migrations values($name)', {
		$name: name
	}, callback);
};

Adapter.prototype.unmarkExecuted = function(name, callback) {
	this.db.run('delete from _migrations where name=$name', {
		$name: name
	}, callback);
};

module.exports = Adapter;
