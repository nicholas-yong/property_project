let express = require('express');
let app = express();
let upload = require("express-fileupload");
let fs = require('fs');
let axios = require('axios');
let { Client } = require('pg');
let client = new Client(
{
    user: 'postgres',
    host: 'localhost',
    database: 'blog',
    password: 'namihana',
    port: 5432,
});

client.connect();

module.exports = client;

let functions = require( "C:/Users/nickh/Documents/GitHub/property_project/Tutor Ver 2/functions.js");
const { isRegExp } = require('util');
const { response } = require('express');

app.set('view engine', 'ejs');

app.use('/assets', express.static('assets'), ( req, res, next ) =>
{
    next();
});

app.use('/webfonts', express.static('assets/webfonts'), ( req, res, next ) =>
{
    next();
});

app.use( upload() );

app.get('/', async ( req, res ) =>
{
    //intialize variables
    let blogData;
    let authorNames = [];
    let tags = [];

    let query_response = [];

    try
    {
        query_response = await client.query( 'SELECT * FROM blogposts b ORDER BY b.postid', blogData );
    }
    catch (error)
    {
        console.error();
    }
    
    //get the name
    for( i = 0 ; i < query_response.rows.length; i++)
    {
        let authorName = [0];
        try 
        {
            await functions.getName( query_response.rows[i].authorid, authorName );
        } 
        catch (error) 
        {
            console.error();
        }
        tags.push( query_response.rows[i].tags.split(','));
        authorNames.push( authorName[0] );
    }
    res.render('pages/home', { data: { blogInfo: query_response.rows, authors : authorNames, blogTags: tags }} );
});

app.get('/contacts', ( req, res ) =>
{
    res.render('pages/contacts');
});

app.get('/projects', (req, res ) =>
{
    res.render('pages/projects');
});

app.get('/properties', (req, res) =>
{
    axios.get('http://localhost:5000/suburbs')
     .then( (response) =>
     {
        res_data = response.data
        suburb_names = new Array();
        for( i = 0; i < res_data.length; i++ )
        {
            suburb_names.push( res_data[i][1] );
        }
        res.render('pages/properties', { data : { suburbNames: suburb_names }});
     })
     .catch( (err) =>
     {
         console.log(err);
     });

});

app.get('/content', async (req, res ) =>
{
    let postid = req.query.id;
    let blogData = {};
    let authorNames = [];
    let tags = [];

    try
    {
        await functions.getBlogItem( postid, blogData );
        await functions.getName( blogData.authorid, authorNames);
        let authorName = authorNames[0];
        let authorObject = {  nameAuthor : authorName };
        Object.assign( blogData, authorObject);
        // get the tags
        tags = blogData.tags.split(',');
        res.render('pages/content', { data: { blogContent: blogData, tagData: tags }});
    }

    catch(e)
    {
        console.log( e );
        res.render( 'pages/404');
    }
});

app.get('/input', (req, res ) =>
{
    res.render('pages/input');
});


app.post("/", async ( req, res ) =>
{
    if( req.files )
    {
        var file = req.files.filename;
        var blog_title = req.body.title;
        var file_data = file.data.toString( 'utf-8');
        var tags = req.body.tags;
        var title_pic = req.body.title_pic;
        var blogData = [file_data, blog_title, 24619, tags, title_pic];
        var returnValue = [0];
        await insertBlogPost( blogData, returnValue );
        if( returnValue[0] == 0)
        {
            functions.updateID('postid');
            res.render('pages/file_success');
        }
        else
        {
            res.render('pages/404');
        }
    }
    else
    {
        console.log( "ERROR when Uploading Text File");
    }
});

async function insertBlogPost( blogData, returnValue )
{
    let ID_Array = [0];
    await functions.getNextID( 'postid', ID_Array );
    let ID = ID_Array[0];
    blogData.push( ID );
    try
    {
        let response = await client.query( `INSERT INTO blogposts( content, title, authorid, tags, title_pic, postid )
        VALUES( $1, $2, $3, $4, $5, $6 )`, blogData );
    }

    catch( err )
    {
        console.log( err );
        returnValue[0] = 1;
    }
}



app.listen('4000');

console.log("We are listening to Port 4000");
