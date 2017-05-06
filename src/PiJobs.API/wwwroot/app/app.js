
(function (angular) {

    var app = angular.module('app', [
        "ngRoute"
    ]);

    app.run(['unloader', function (unloader) {
    }]);
    
    app.config(function ($routeProvider) {
            $routeProvider
                .when("/", {
                    templateUrl: "/routes/landing.html",
                    controller: 'landingCtrl'
                })
                .when("/session", {
                    templateUrl: "/routes/session.html",
                    controller: 'SessionCtrl'
                })

                ;
    });

    app.controller('landingCtrl', ['$scope', 'sessionService', '$location',
        function ($scope, sessionService, $location) {

            function getRandomId() {
                return Math.floor(Math.random() * 90000) + 10000;
            }

            sessionService.createSession('000001', getRandomId(), getRandomId()).then(function (session) {
                sessionService.setSession(session);
                $location.path("/session");
            });


    }]);

    app.controller('SessionCtrl', ['$scope', 'sessionService', '$timeout', '$location',
        function ($scope, sessionService, $timeout, $location) {

        $scope.digitOptions = ['100', '200', '1000', '10000'];
        $scope.digitOption = '1000';

        $scope.session = sessionService.getSession();

        if ($scope.session == null) {
            $location.path("/");
        }

        $scope.data = [];
        $scope.currentJob = {};
        $scope.jobRunning = false;

        $scope.submitJob = function () {
            $scope.jobRunning = true;
            sessionService.submitJob($scope.digitOption).then(function () {
                $scope.currentJob = {
                    digits: $scope.digitOption,
                    value: "3....",
                    progress: 0
                };
                $scope.data.push($scope.currentJob);
                $timeout(checkJob, 1000);
            });
        }

        function checkJob() {
            sessionService.getJobStatus().then(function (status) {
                if (status == 4) {
                    sessionService.removeJob().then(function () {
                        $scope.currentJob.value = "3.14";
                        $scope.currentJob.progress = 100;
                        $scope.data[$scope.data.length - 1] = $scope.currentJob;
                        $scope.jobRunning = false;
                        $scope.currentJob = {};

                    });
                }
                else {
                    $timeout(checkJob, 1000);
                }
            });
        }

    }]);


})(angular);