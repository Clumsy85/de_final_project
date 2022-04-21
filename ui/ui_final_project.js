let keywordsInput = ''
let marketInput = ''
let fromDateInput = ''
let toDateInput = ''
let categoryInput = ''
let countryInput = ''
submitInput = {}
var server = "http://127.0.0.1:5000/"

console.log('hello from js')

document.querySelector('form.jotform-form').addEventListener('change', function (e) {

    //prevent the normal submission of the form
    e.preventDefault();
	  keywordsInput = $("form").serializeArray()[1].value
	  fromDateInput = document.getElementById("lite_mode_21")
    toDateInput = document.getElementById("lite_mode_27")
    categoryInput = $("form").serializeArray()[8].value
    countryInput = $("form").serializeArray()[9].value

    userInputs = {
      "keywords" : keywordsInput,
      "From Date" : fromDateInput.value,
      "To Date" : toDateInput.value,
      "category" : categoryInput,                
      "country" : countryInput                  
      
      }
    console.log(userInputs);    
});

function sendUserInfo() {
  let userInfo = userInputs
  const request = new XMLHttpRequest()
  request.open('POST', `/ui_submit/${JSON.stringify(userInfo)}`)
  request.onload = () => {
    const flaskMessage = request.responseText
    console.log(flaskMessage)
  }
  request.send()
}

function SendUserInfoAJAX () {
  $.ajax({
    url: '/ui_submit',
    type: "POST",
    dataType: "json",
    contentType: 'application/json;charset=UTF-8',
    data: JSON.stringify(userInputs),
    success: function(data) {
      console.log('done')
    }
  })}


// $('.button').on('click', function() {
//   submitInput = userInputs
//   submitInput = JSON.stringify(submitInput)
// })



// $('.button').on('click', function() {
//   var $self = $(this);  
//     console.log('submitted')
//     SendUserInfoAJAX()

// })


document.getElementById('input_20').onclick  = ( function() {
	console.log('submitted')
    SendUserInfoAJAX()

});