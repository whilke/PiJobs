
(function (angular) {

    var app = angular.module('app', [
        "ngRoute"
    ]);
    
    app.config(function ($routeProvider) {
            $routeProvider
                .when("/", {
                    templateUrl: "./routes/landing.html",
                    controller: 'landingCtrl'
                })
                
                ;
    });

    app.controller('landingCtrl', ['$scope', 'adminService', '$timeout',
        function ($scope, adminService, $timeout) {

            $scope.accounts = [];
            function updateAccounts() {

                adminService.getAccountStats().then(function (data) {
                    $scope.accounts = data;

                    $timeout(updateAccounts, 1000);
                });
            };

            updateAccounts();

    }]);

})(angular);