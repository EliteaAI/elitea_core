$("#btn-update").click(function() {
  axios.post(elitea_ui_api_url, {})
    .then(function (response) {
      showNotify("SUCCESS", "EliteaUI update requested")
    })
    .catch(function (error) {
      showNotify("ERROR", "Error during EliteaUI update request")
      console.log(error);
    });
});
