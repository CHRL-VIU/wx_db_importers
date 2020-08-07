<?php

$url_array = [
    plummer => 'https://pvs.nupointsystems.com/latest2.php?pass=%F3uq%2C%F7%85c%D7%2A%10%ED%17%E2i%0A%00',
    klinaklini => 'https://pvs.nupointsystems.com/latest2.php?pass=%F3uq%2C%F7%85c%D7%2A0%9D%E5%5B%01%00',
    homathko => 'https://pvs.nupointsystems.com/latest2.php?pass=%F3uq%2C%F7%85c%D7%2A%10%ED%E7%E2h%0A%00',
    perseverance => 'https://pvs.nupointsystems.com/latest2.php?pass=%F3uq%2C%F7%85c%D7%2A0%9D%95%5E%0E%00'
    ];

$filename_array = [
    plummer => "/home/viuhydro/public_html/webcam_images/plummer/Plummer_VIU_UNBC_HAKAI__",
    klinaklini => "/home/viuhydro/public_html/webcam_images/klinaklini/VIU_UNBC_Hakai_Klin_",
    homathko => "/home/viuhydro/public_html/webcam_images/homathko/Homathko_VIU_UNBC_Hakai_",
    perseverance => "/home/viuhydro/public_html/webcam_images/perseverance/ComoxRD_13000280_"
    ];
    
$meta_array = [
    plummer => 'https://pvs.nupointsystems.com/metadata.php?pass=%F3uq%2C%F7%85c%D7%2A%10%ED%17%E2i%0A%00',
    klinaklini => 'https://pvs.nupointsystems.com/metadata.php?pass=%F3uq%2C%F7%85c%D7%2A0%9D%E5%5B%01%00',
    homathko => 'https://pvs.nupointsystems.com/metadata.php?pass=%F3uq%2C%F7%85c%D7%2A%10%ED%E7%E2h%0A%00',
    perseverance => 'https://pvs.nupointsystems.com/metadata.php?pass=%F3uq%2C%F7%85c%D7%2A0%9D%95%5E%0E%00'
    ];

   
// loop through url array stn name is key value is url then use the stn name to query the filename array    
foreach($url_array as $key => $value){
 //Get the file
$content = file_get_contents($value);
// Get date taken 
$meta = file_get_contents($meta_array[$key]);
$pos = strpos($meta, 'date_time');
$str = preg_replace('/[^0-9]/', '', substr($meta, $pos+13, 20));

//Store in the filesystem.
$fp = fopen($filename_array[$key].$str.".jpg", "w");
fwrite($fp, $content);
fclose($fp);
}

?>