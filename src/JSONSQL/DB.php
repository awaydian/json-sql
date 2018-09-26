<?php
/**
 * @author Away.D
 *
 */

namespace JSONSQL;

class DB
{
    private $cfgPath;
    private $parser;

    function __construct($cfgPath){
        if (substr($cfgPath, -1) != '/') {
            $cfgPath .= '/';
        }
        $this->cfgPath = $cfgPath;
        $this->parser = new Parser();
    }

    private function parseSql($sql){
        return $this->parser->parse($sql);
    }

    public function query($sql){
        $parsed = $this->parseSql($sql);

        // print_r($parsed);

        if (!empty($parsed['SELECT'])) {
            return $this->execSelect($parsed);
        } elseif (!empty($parsed['DELETE'])) {
            return $this->execDelete($parsed);
        } elseif (!empty($parsed['UPDATE'])) {
            return $this->execUpdate($parsed);
        }
    }

    private function execSelect($parsed) {
        $tables = $this->getParsedTable($parsed['FROM']);
        $tablesData = $this->getParsedTableData($tables);

        $wheres = isset($parsed['WHERE']) ? $parsed['WHERE'] : [];
        $conditions = $this->getParsedCondition($wheres);
        $tablesData = $this->getConditionFilterResult($tablesData, $conditions);
        foreach ($conditions as $condition) {
            $keys = $condition['key'];
            $tab = null;
            if (in_array($keys[0], array_keys($tablesData))) {
                $tab = $keys[0];
                unset($keys[0]);
                $keys = array_values($keys);
            }
            foreach ($tablesData as $table => $data) {
                if ($tab != null && $table != $tab) {
                    continue;
                }
                // find target then filter
                foreach ($data as $_key => $record) {
                    $record['_key'] = $_key;

                    $stack = [];
                    $stack[] = $table;
                    $stack[] = & $tablesData[$table];
                    $stack[] = '_key';
                    $stack[] = & $tablesData[$table][$_key];
                    
                    $target = & $tablesData[$table][$_key];
                    foreach ($keys as $key) {
                        $target = & $target[$key];
                        $stack[] = $key;
                        $stack[] = $target;
                    }

                    // filter
                    $status = true;
                    switch (strtoupper($condition['operator'])) {
                        case '=':
                            $status = $target == $condition['value'];
                            break;
                        case '!=':
                        case '<>':
                            $status = $target != $condition['value'];
                            break;
                        case '>':
                            $status = $target > $condition['value'];
                            break;
                        case '>=':
                            $status = $target >= $condition['value'];
                            break;
                        case '<':
                            $status = $target < $condition['value'];
                            break;
                        case '<=':
                            $status = $target <= $condition['value'];
                            break;
                        case 'BETWEEN':
                            $status = $target >= $condition['value'][0];
                            $status = $target <= $condition['value'][1];
                            break;
                        case 'IN':
                            $status = in_array($target, $condition['value']);
                            break;
                        
                        default:
                            break;
                    }

                    if (!$status) {
                        $findKey = $stack[count($stack) - 4];
                        if ($findKey == '_key') {
                            $tmp = & $stack[count($stack) - 5];
                            unset($tmp[$_key]);
                        }
                    }
                }
            }
        }

        foreach ($tables as $table => $alias) {
            if ($table != $alias) {
                unset($tablesData[$table]);
            }
        }

        $columns = [];
        foreach ($parsed['SELECT'] as $select) {

            $col = empty($select['no_quotes']) ? $column['column'] = $select['base_expr'] : $select['no_quotes']['parts'][0];

            $columns[$col] = $col;

            if (!empty($select['alias'])) {
                $alias = $select['alias']['no_quotes']['parts'][0];
                $columns[$col] = $alias;
            }
        }

        foreach ($columns as $column => $alias) {
            if ($column  == '*') {
                continue;
            }
            if ($column  == '_key') {
                foreach ($tablesData as $table => $data) {
                    foreach ($data as $key => $record) {
                        if (empty($record['_key'])) {
                            $tablesData[$table][$key]['_key'] = $key;
                        }
                    }
                }
            }
        }

        return $tablesData;
    }

    private function execDelete($parsed) {
        if (empty($parsed['FROM'])) {
            return false;
        }

        $tables = $this->getParsedTable($parsed['UPDATE']);
        $tablesData = $this->getParsedTableData($tables);
        $wheres = isset($parsed['WHERE']) ? $parsed['WHERE'] : [];
        $conditions = $this->getParsedCondition($wheres);
        $tablesData = $this->getConditionFilterResult($tablesData, $conditions, true);

        foreach ($tables as $table => $alias) {
            if ($table != $alias) {
                unset($tablesData[$table]);
            }
        }

        // lock 
        // write
        foreach ($tables as $table => $alias) {
            $this->setTableData($table, $tablesData[$table]);
        }

    }

    private function getParsedTable($parsedTable) {
        $tables = [];
        foreach ($parsedTable as $from) {
            // if ($from['expr_type'] == 'table') {
            $tab = empty($from['no_quotes']) ? $from['table'] : $from['no_quotes']['parts'][0];

            $tables[$tab] = $tab;

            if (!empty($from['alias'])) {
                $alias = $from['alias']['no_quotes']['parts'][0];
                $tables[$tab] = $alias;
            }
        }
        return $tables;
    }

    private function getParsedTableData($tables) {
        $tablesData = [];
        foreach ($tables as $table => $alias) {
            // if ($from['expr_type'] == 'table') {

            $fileData = $this->getTableData($table);
            $tablesData[$table] = & $fileData;

            if ($table != $alias) {
                $tablesData[$alias] = & $fileData;
            }
        }
        return $tablesData;
    }

    private function getParsedCondition($parsedCondition) {
        // where
        $conditions = [];
        $conditionIndex = -1;
        foreach ($parsedCondition as $word) {
            switch ($word['expr_type']) {
                case 'colref':
                    $conditionIndex++;
                    $conditions[$conditionIndex] = [];
                    $keys = empty($word['no_quotes']['parts']) ? $word['base_expr'] : $word['no_quotes']['parts'];
                    $conditions[$conditionIndex]['key'] = $keys;
                    break;
                case 'operator':
                    $conditions[$conditionIndex]['operator'] = $word['base_expr'];
                    break;
                case 'const':
                    $conditions[$conditionIndex]['value'] =  $this->parseStringColon($word['base_expr']);
                    break;
                case 'in-list':
                    $value = $word['base_expr'];
                    $first = substr($value, 0, 1);
                    $last = substr($value, -1);
                    if ($first == '(' && $last == ')') {
                        $value = substr($value, 1, strlen($value) - 2);
                        $value = explode(',', $value);
                        foreach ($value as $key => $single) {
                            $value[$key] = $this->parseStringColon(trim($single));
                        }
                    }
                    $conditions[$conditionIndex]['value'] = $value;

                    break;
                default:
                    break;
            }
        }
        return $conditions;
    }

    private function getConditionFilterResult($tablesData, $conditions, $negative = false) {
        foreach ($conditions as $condition) {
            $keys = $condition['key'];
            $tab = null;
            if (in_array($keys[0], array_keys($tablesData))) {
                $tab = $keys[0];
                unset($keys[0]);
                $keys = array_values($keys);
            }
            foreach ($tablesData as $table => $data) {
                if ($tab != null && $table != $tab) {
                    continue;
                }
                // find target then filter
                foreach ($data as $_key => $record) {
                    $record['_key'] = $_key;

                    $stack = [];
                    $stack[] = $table;
                    $stack[] = & $tablesData[$table];
                    $stack[] = '_key';
                    $stack[] = & $tablesData[$table][$_key];
                    
                    $target = & $tablesData[$table][$_key];
                    $target['_key'] = $_key;

                    foreach ($keys as $key) {
                        $target = & $target[$key];
                        $stack[] = $key;
                        $stack[] = $target;
                    }

                    // filter
                    $status = true;
                    switch (strtoupper($condition['operator'])) {
                        case '=':
                            $status = $target == $condition['value'];
                            break;
                        case '!=':
                        case '<>':
                            $status = $target != $condition['value'];
                            break;
                        case '>':
                            $status = $target > $condition['value'];
                            break;
                        case '>=':
                            $status = $target >= $condition['value'];
                            break;
                        case '<':
                            $status = $target < $condition['value'];
                            break;
                        case '<=':
                            $status = $target <= $condition['value'];
                            break;
                        case 'BETWEEN':
                            $status = $target >= $condition['value'][0];
                            $status = $target <= $condition['value'][1];
                            break;
                        case 'IN':
                            $status = in_array($target, $condition['value']);
                            break;
                        
                        default:
                            break;
                    }
                    $status = $negative ? !$status : $status;
                    if (!$status) {
                        $findKey = $stack[count($stack) - 4];
                        if ($findKey == '_key') {
                            $tmp = & $stack[count($stack) - 5];
                            unset($tmp[$_key]);
                        }
                    }
                }
            }
        }
        return $tablesData;
    }

    private function execUpdate($parsed) {
        $tables = $this->getParsedTable($parsed['UPDATE']);
        $tablesData = $this->getParsedTableData($tables);
        $conditions = $this->getParsedCondition($parsed['WHERE']);
        $targetTablesData = $this->getConditionFilterResult($tablesData, $conditions);

        $sets = [];
        foreach ($parsed['SET'] as $exp) {
            $sets = array_merge($sets, $this->getParsedCondition($exp['sub_tree']));
        }

        foreach ($sets as $condition) {
            $keys = $condition['key'];
            $tab = null;
            if (in_array($keys[0], array_keys($tablesData))) {
                $tab = $keys[0];
                unset($keys[0]);
                $keys = array_values($keys);
            }
            foreach ($targetTablesData as $table => $data) {
                if ($tab != null && $table != $tab) {
                    continue;
                }
                foreach ($data as $_key => $record) {
                    $record['_key'] = $_key;

                    $stack = [];
                    $stack[] = $table;
                    $stack[] = & $tablesData[$table];
                    $stack[] = '_key';
                    $stack[] = & $tablesData[$table][$_key];
                    
                    $target = & $tablesData[$table][$_key];
                    foreach ($keys as $key) {
                        $target = & $target[$key];
                        $stack[] = $key;
                        $stack[] = $target;
                    }
                    switch (strtoupper($condition['operator'])) {
                        case '=':
                            $target = $condition['value'];
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // lock 
        // write
        foreach ($tables as $table => $alias) {
            // echo $table,"\n";
            // print_r($tablesData[$table]);
            $this->setTableData($table, $tablesData[$table]);
        }

    }

    private function getFilePathByTable($table){
        if (substr($table, 0, 1) == '/') {
            $table = substr($table, 1);
        }
        return $this->cfgPath . $table . '.json';
    }

    // validate cols
    // validate table
    private function getTableData($table){
        return $this->getFileContent($this->getFilePathByTable($table));
    }

    private function setTableData($table, &$data){
        return file_put_contents($this->getFilePathByTable($table), json_encode($data));
    }

    private function getWhereResult(&$allData, $conditions)
    {
        // print_r($conditions);
        foreach ($conditions as $condition) {
            if ($condition == '1') {
                continue;
            }
        }
    }
    // filter colums
    private function filterAllData(&$allData, $targets){
        if ( count($targets) == 1 && $targets[0] == '*' ) {
            return;
        }
        foreach ($allData as $table => $data) {
        }
    }
    // read lock
    private function getFileContent($filePath){
        if (is_dir($filePath))
        {
            if (in_array($filePath, ['.', '..'])) {
                return null;
            }
            $content = scandir($filePath);
            unset($content[array_search('.', $content)]);
            unset($content[array_search('..', $content)]);
        }
        $content = json_decode(file_get_contents($filePath), 1);
        return $content;
    }

    private function parseStringColon($value){
        $first = substr($value, 0, 1);
        $last = substr($value, -1);

        if (in_array($first, ['\'','"']) && ($first == $last)) {
            $value = substr($value, 1, strlen($value) -2);
        }
        return $value;
    }


    public function bind($sql, $params){

    }
}