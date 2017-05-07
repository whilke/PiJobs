angular.module('app')
    .service('sessionService', ['$http', function ($http) {

        var _session = {Account: 0, UserId: 0, DataId: 0};

        this.setSession = function (session) {
            _session = session;
        };
        this.getSession = function () {
            return _session;
        };

        this.createSession = function (account, user, data) {

            return $http.get('/api/session/' + account + '/' + user + '/' + data)
                .then(function (response) {
                    return response.data;
                }, function () {
                    return _session;
                });
        };

        this.getData = function () {
            return $http.get('/api/session/data/'
                + _session.Account + '/' + _session.UserId + '/' + _session.DataId)
                .then(function (response) {
                    return response.data;
                }, function () {
                    return null;
                });
        }

        this.getQueueSize = function () {
            return $http.get('/api/session/queue/'
                + _session.Account + '/' + _session.UserId + '/' + _session.DataId)
                .then(function (response) {
                    return response.data;
                }, function () {
                    return null;
                });
        }

        this.close = function () {
            if (_session === null) return;

            var xmlhttp = new XMLHttpRequest();
            xmlhttp.open("DELETE"
                , '/api/session/' + _session.Account + '/' + _session.UserId + '/' + _session.DataId
                , false);//the false is for making the call synchronous
            xmlhttp.setRequestHeader("Content-type", "application/json");
            xmlhttp.send();
        };

        this.submitJob = function (digits) {

            return $http.post('/api/job/'
                + _session.Account + '/' + _session.UserId + '/' + _session.DataId + '/' + digits)
                .then(function (response) {
                    return;
                }, function () {
                    return;
                });
        };

        this.getJobStatus = function () {

            return $http.get('/api/job/' + _session.Account + '/' + _session.UserId + '/' + _session.DataId)
                .then(function (response) {
                    return response.data;
                }, function () {
                    return "";
                });
        };

        this.removeJob = function () {

            return $http.delete('/api/job/' + _session.Account + '/' + _session.UserId + '/' + _session.DataId)
                .then(function (response) {
                    return;
                }, function () {
                    return;
                });
        };

}]);