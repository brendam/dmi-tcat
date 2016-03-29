<?php
require_once '../capture/common/functions.php';

class CaptureFunctionsTest extends PHPUnit_Framework_TestCase
{
    public function test_ratelimit_report_problem()
    {

        define('RATELIMIT_MAIL_HOURS', '24');
        # need to mock the database to test this function!
        # and mail function - what we need to know is if 'mail' is called, but don't want it actually called.
        $this->assertEquals(ratelimit_report_problem);

    }
}
?>