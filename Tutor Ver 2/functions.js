var client = require('./app');

let getNextID = function( parameter, value )
{
    return new Promise( (resolve, reject) => 
    {
        client.query( 'SELECT parvalue + incval FROM keytable WHERE parameter = $1', [parameter], ( err, res ) =>
        {
            if( err )
            {
                value[0] = 0;
                reject();
            }

            else
            {
                value[0] = Object.values( res.rows[0] )[0];
                resolve();
            }
            });
    });
}

let updateID = function( parameter )
{
    return new Promise( ( resolve, reject ) =>
    {
        client.query( 'UPDATE keytable SET parvalue = parvalue + incval WHERE parameter = $1', [parameter], (err, res) =>
        {
            if( err )
            {
                console.log( err );
                reject();
            }

            else
            {
                resolve();
            }
        });
    });
}


let getName =  function ( conid, name )
{
    return new Promise( (resolve, reject) =>
    {
        client.query( `SELECT n.name FROM names n WHERE conid = $1`, [conid], ( err, res ) =>
        {
            if( err )
            {
                name[0] = null;
                reject();
            }

            else
            {
                name[0] = Object.values( res.rows[0])[0];
                resolve();
            }
        });
    });
}

let getBlogItem = function ( postid, data )
{
    return new Promise( ( resolve, reject) =>
    {
        console.log( postid );
        client.query( 'SELECT * FROM blogposts bp WHERE bp.postid = $1', [postid], (err, res) =>
        {
            if (err)
            {
                data = null;
                reject();
            }
            else
            {
                Object.assign( data, res.rows[0] );
                resolve()
            }
        });
    });
}

exports.getNextID = getNextID;
exports.updateID = updateID;
exports.getName = getName;
exports.getBlogItem = getBlogItem;