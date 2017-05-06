angular.module('app')
    .factory('unloader', ['$rootScope', '$window', 'sessionService',
        function ($rootScope, $window, sessionService) {

            $window.onbeforeunload = function (e) {
                sessionService.close();
                console.log('session closed');
            };

            return {};
        }]);
