
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

            function getAccountId() {
                var r = Math.floor(Math.random() * 3);
                if (r === 0) return '000001'
                if (r === 1) return '000002'
                if (r === 2) return '000003'
                return '000004'
            }

            sessionService.createSession(getAccountId(), getRandomId(), getRandomId()).then(function (session) {
                sessionService.setSession(session);
                $location.path("/session");
            });


    }]);

    app.controller('SessionCtrl', ['$scope', 'sessionService', '$timeout', '$location',
        function ($scope, sessionService, $timeout, $location) {

        $scope.digitOptions = ['1000', '10000', '50000', '100000'];
        $scope.digitOption = '1000';

        $scope.session = sessionService.getSession();
       
        $scope.queueSize = 0;
        function updateQueueSize() {

            sessionService.getQueueSize().then(function (size) {
                $scope.queueSize = size;
                $timeout(updateQueueSize, 1000);
            });
        }
        updateQueueSize();

        if ($scope.session.Account === 0) {
            $location.path("/");
        }

        $scope.data = [];
        $scope.currentJob = {};
        $scope.jobRunning = false;

        $scope.startTime = null;
        $scope.stopTime = null;
        $scope.submitJob = function () {
            $scope.jobRunning = true;
            $scope.startTime = new Date();
            $scope.stopTime = null;
            $scope.status = 0;
            sessionService.submitJob($scope.digitOption).then(function () {
                $scope.currentJob = {
                    digits: $scope.digitOption,
                    value: "",
                    progress: 0
                };
                $scope.data.push($scope.currentJob);
                updateView();
                $timeout(checkJob, 1000);
            });
        };
        $scope.status = 0;
        function checkJob() {
            sessionService.getJobStatus().then(function (status) {
                $scope.status = status;
                if (status === 4) {
                    $scope.status = 3;
                    sessionService.removeJob().then(function () {
                        sessionService.getData().then(function (result) {
                            $scope.status = 4;
                            $scope.currentJob.value = result.substr(0, 10)
                                + "..."
                                + result.substr(result.length - 10);
                            $scope.currentJob.progress = 100;
                            $scope.currentJob.currentTime = $scope.runningTime();
                            $scope.currentJob.currentStatus = $scope.JobState();
                            $scope.data[$scope.data.length - 1] = $scope.currentJob;
                            $scope.jobRunning = false;
                            $scope.currentJob = {};
                            $scope.stopTime = new Date();

                            $timeout.cancel(viewTimer);
                        });
                    });
                }
                else {
                    $timeout(checkJob, 1000);
                }
            });
        }

        var viewTimer = 0;
        function updateView() {
            
            $scope.currentJob.currentTime = $scope.runningTime();
            $scope.currentJob.currentStatus = $scope.JobState();
            console.log($scope.currentJob.currentTime);
            viewTimer = $timeout(updateView, 100);
        }

        $scope.runningTime = function () {
            if ($scope.startTime === null) {
                return "";
            }

            var endTime = $scope.stopTime;
            if (endTime === null) {
                endTime = new Date();
            }

            var diff = endTime.getTime() - $scope.startTime.getTime();
            var seconds = diff / 1000;
            return Math.round(seconds, 2) + " seconds";
        };
        $scope.JobState = function () {
            if ($scope.status === 3) return "RUNNING";
            if ($scope.status === 4) return "FINISHED";
            return "QUEUED";
        };

    }]);


})(angular);