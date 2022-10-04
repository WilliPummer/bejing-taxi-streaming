/* eslint-disable no-undef */


// A $( document ).ready() block.
$( document ).ready(function() {
    $("#buttonStart").click(function(){
        $.ajax({
          type: 'POST',
          url: "/start",
          dataType: "json",
          success: function(data){
                    console.log(data)
                    document.getElementById("status").innerText = data.status
          }
        });
    });

    $("#buttonStop").click(function(){
        $.ajax({
          type: 'POST',
          url: "/stop",
          dataType: "json",
          success: function(data){
                    console.log(data)
                    document.getElementById("status").innerText = data.status
          }
        });
    });

    $("#buttonSilence").click(function(){
        $.ajax({
          type: 'POST',
          url: "/silence",
          dataType: "json",
          success: function(data){
                    console.log(data)
                    document.getElementById("status").innerText = data.status
          }
        });
    });

    function getCounter() {
        $.ajax({
          type: 'GET',
          url: "/index",
          dataType: "json",
          success: function(data){
                    console.log(data)
                    document.getElementById("index").innerText = "Current Index: " + data.index
          }
        });
        setTimeout(getCounter, 1000);

    }

    setTimeout(getCounter, 1000);


});

