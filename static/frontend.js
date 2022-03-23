// frontend js functions

function setDefaultDates() {
    var startDateControl = document.querySelector('input[id="start-date"]');
    var endDateControl = document.querySelector('input[id="end-date"]');
    // first day of last year
    var startDate = new Date(new Date().getFullYear() -1, 0, 1);
    // end of the year
    var now = new Date();
    if (now.getMonth() == 11) {
        var endDate = new Date(now.getFullYear() + 1, 0, 1);
    } else {
        var endDate = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    }
    startDateControl.value = startDate.toISOString().split('T')[0];
    endDateControl.value = endDate.toISOString().split('T')[0];
}
