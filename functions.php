<?php

function getMySQLRows($conn, $stationName, $numRows) {

    $sql = "(SELECT * FROM `$stationName` ORDER BY DateTime desc LIMIT $numRows) order by DateTime";

    $result = mysqli_query($conn,$sql);

    if(!$result){
        exit("Select Query Error Description: ". mysqli_error($conn));
    }

    // put query in assoc array 
    $raw_array = array();

    while($row = mysqli_fetch_assoc($result)) {
        $raw_array[] = $row;
        }

    return $raw_array;
}

?>