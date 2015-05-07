begin transaction;

create temp table ${staging} (like ${table});

copy ${staging} 
from '${s3DataPath}'
credentials 'aws_access_key_id=${accessKey};aws_secret_access_key=${secretKey}'
json 's3://${bucket}/${jsonPathsS3Path}'
gzip acceptanydate timeformat 'auto';

delete from ${table} 
using ${staging} 
where
	${primaryKeyCheck};

insert into ${table} 
select * from ${staging};

drop table ${staging};
end transaction;