angular.module('app')
    .service('adminService', ['$http', function ($http) {

        this.getAccountStats = function () {

            return $http.get('/admin/api/accountQueues')
                .then(function (response) {
                    return response.data;
                }, function () {
                    return null;
                });
        };

    }]);