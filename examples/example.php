<?php
namespace JSONSQL;
require_once dirname(__FILE__) . '/../vendor/autoload.php';

$db = new DB(dirname(__FILE__) . '/json_files');

$sql = "select  *  from `nodes` as `n`";
print_r($db->query($sql));

$sql = "select  * , _key as `id`  from `nodes` as `n` where `nodes`.`_key` = '24642684-3b34-45f5-8d8b-20e281ee7f32'";
print_r($db->query($sql));

$sql = "select  * from `nodes` as `n` where n.device_uid = '7d56269a-48f0-4908-a8a9-1156a8ec4930'";
print_r($db->query($sql));

$sql = "select  * from `/nodes` as `n` where device_uid <> '7d56269a-48f0-4908-a8a9-1156a8ec4930'";
print_r($db->query($sql));

$sql = "select  * from `nodes` as `n` where device_uid in ('7d56269a-48f0-4908-a8a9-1156a8ec4930', '57414b7d-e354-407b-828c-ff38f318eea0')";
print_r($db->query($sql));


// $sql = "delete from `nodes` where device_uid = '7d56269a-48f0-4908-a8a9-1156a8ec4930'";
// $sql = "delete from `nodes` where device_uid in ('7d56269a-48f0-4908-a8a9-1156a8ec4930', '57414b7d-e354-407b-828c-ff38f318eea0')";

// $sql = "update `nodes` set `pos_x` = 111, `pos_y` = 222 where `device_uid` = '7d56269a-48f0-4908-a8a9-1156a8ec4930' ";


