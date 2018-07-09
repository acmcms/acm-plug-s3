var dbi = require('ru.acmcms/dbi');

exports.doCleanDeadIndices = function(){
	dbi.executeUpdate('DELETE FROM s3Indices WHERE');
};