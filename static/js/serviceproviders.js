$(".refresh-table-button").click(function() {
  $($(this).data("target")).bootstrapTable("refresh", {});
});


$("#modal-adddescriptor").on("show.bs.modal", function (e) {
  $("#input-project-id").val("1");
  $("#input-descriptor").val("");
});


$("#btn-register-submit").click(function() {
  var project_id = $("#input-project-id").val();
  var descriptor = $("#input-descriptor").val();
  //
  axios.post(register_url + project_id, JSON.parse(descriptor))
    .then(function (response) {
      $("#table-serviceproviders").bootstrapTable("refresh", {});
      //
      showNotify("SUCCESS", "Descriptor registered")
      $("#modal-adddescriptor").modal("hide");
    })
    .catch(function (error) {
      showNotify("ERROR", "Error during descriptor registration")
      console.log(error);
    });
});


function actions_formatter(value, row, index) {
  return [
    '<a class="task-unregister" href="javascript:void(0)" title="Remove">',
    '  <i class="fa fa-trash" style="color: #858796"></i>',
    '</a>',
  ].join('')
}


window.actions_events = {
  "click .task-unregister": function (e, value, row, index) {
    if (!window.confirm("Remove " + row.project_id + ":" + row.provider_name + ":" + row.service_location_url + "?")) {
      return;
    }
    //
    axios.delete(register_url + row.project_id, {params: {provider_name: row.provider_name, service_location_url: row.service_location_url}})
      .then(function (response) {
        if (response.data.ok) {
          $("#table-serviceproviders").bootstrapTable("refresh", {});
          //
          showNotify("SUCCESS", "Removal requested");
        } else {
          showNotify("ERROR", response.data.error);
        }
      })
      .catch(function (error) {
        showNotify("ERROR", "Error during removal request");
        console.log(error);
      });
  }
}
