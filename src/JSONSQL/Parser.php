<?php
namespace JSONSQL;

use PHPSQLParser\PHPSQLParser;
use PHPSQLParser\PHPSQLCreator;

class Parser {

    private $parser;
    function __construct()
    {
        $this->parser = new PHPSQLParser();
    }

    public function parse($sql)
    {
        return $this->parser->parse($sql);
    }

}

// SELECT
// INSERT
// UPDATE
// DELETE

// REPLACE
// RENAME
// SHOW
// SET
// DROP
// CREATE INDEX
// CREATE TABLE
// EXPLAIN
// DESCRIBE
// $creator = new PHPSQLCreator($parsed);
// echo $creator->created;