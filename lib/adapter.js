'use strict';

var sqlite3 = require('sqlite3'),
	path = require('path'),
	Steppy = require('twostep').Steppy;

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
			callback(null, {db: db, replaceColumn: replaceColumn});
		});
	});
};

Adapter.prototype.disconnect = function(callback) {
	this.db.close(callback);
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

// Helpers
function replaceColumn(params, callback) {
	var db = params.db,
		tableName = params.tableName,
		newName = tableName + '1',
		newColumnName = params.newColumnName || params.columnName,
		oldColumnName = params.columnName,
		definitionRegExp = '.*?(?=,|\\)$)';
	Steppy(
		// open transaction
		function() {
			db.run('savepoint migration13', this.slot());
		},

		// rename old table
		function() {
			db.run(
				'alter table ' + tableName +
				' rename to ' + newName + ';',
				this.slot()
			);
		},

		// get table sql
		function(err) {
			db.get(
				'select * from sqlite_master where type=$type ' +
				'and name=$name;',
				{
					$type: 'table',
					$name: newName
				},
				this.slot()
			);
		},

		// create new table
		function(err, rec) {
			// construct regexp for searching column
			var oldColRegexp = new RegExp(
				oldColumnName + '\\s(' + definitionRegExp + ')', 'i'
			);

			// replace temporary table name in sql with good old name
			var sql = rec.sql.replace(newName, tableName);

			if (params.columnDefinition) {
				// replace column with new definition
				sql = sql.replace(oldColRegexp, newColumnName + ' ' +
					params.columnDefinition);
			} else if (params.removeColumn) {

				// remove column (obviously)
				sql = sql.replace(oldColRegexp, '')
					.replace(/(,\s*,)|(,\s*\))/, function(str) {
					return str.slice(-1);
				});
			} else {

				// just rename column, left it with old definition
				sql = sql.replace(oldColRegexp, function(str, definition) {
					return newColumnName + ' ' + definition;
				});
			}

			// create new table
			this.pass(sql);
			db.run(sql, this.slot());
		},

		// fill new table with data
		function(err, sqlString) {
			var newColumnsString,
				oldColumnsString;
			if (params.copyColumn) {
				if (newColumnName === oldColumnName) {
					newColumnsString = '*';
					oldColumnsString = '*';
				} else {
					newColumnsString = getColumnNames(sqlString, tableName).join(', ');
					oldColumnsString = newColumnsString.replace(
						newColumnName, oldColumnName
					);
				}
			} else {
				var columns = getColumnNames(sqlString, tableName);
				var delColInd = columns.indexOf(oldColumnName);
				columns = [].concat(
					columns.slice(0, delColInd), columns.slice(delColInd + 1)
				);
				newColumnsString = columns.join(', ');
				oldColumnsString = newColumnsString;
			}

			// copy from one table to another
			db.run(
				'insert into ' + tableName +
				(newColumnsString === '*' ? '' : ('(' + newColumnsString + ')')) +
				' select ' + oldColumnsString +
					' from ' + newName + ';',
				this.slot()
			);
		},

		// remove old table
		function() {
			db.run('drop table ' + newName + ';', this.slot());
		},

		//close transaction
		function() {
			db.run('release savepoint migration13', this.slot());
		},

		callback
	);
}

// return column names from create table sql
function getColumnNames(sql, tableName) {
	var regExp = new RegExp('(?:^create\\stable\\s"?' + tableName
		+ '"?\\s\\()(.*)(?:\\)$)', 'i');
	var columns = (regExp.exec(sql)[1]).split(',');
	return columns.map(function(column) {
		return /^(?:\s)?([^\s]+)/.exec(column)[1];
	});
}

module.exports = Adapter;
