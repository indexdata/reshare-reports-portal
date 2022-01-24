// frontend js functions

function setDefaultDates() {
    var startDateControl = document.querySelector('input[id="start-date"]');
    var endDateControl = document.querySelector('input[id="end-date"]');
    // first day of last year
    var startDate = new Date(new Date().getFullYear() -1, 0, 1);
    // end of the year
    var endDate = new Date(new Date().getFullYear(), 0, 365);
    startDateControl.value = startDate.toISOString().split('T')[0];
    endDateControl.value = endDate.toISOString().split('T')[0];
}

