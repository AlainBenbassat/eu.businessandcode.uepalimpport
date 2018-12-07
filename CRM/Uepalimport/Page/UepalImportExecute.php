<?php
use CRM_Uepalimport_ExtensionUtil as E;

class CRM_Uepalimport_Page_UepalImportExecute extends CRM_Core_Page {

  public function run() {
    CRM_Utils_System::setTitle(E::ts('Uepal Import - Execute'));

    $msg = '';
    $action = CRM_Utils_Array::value('action', $_GET);

    $importHelper = new CRM_Uepalimport_Helper();

    try {
      if ($action == 'checkConfig') {
        $msg = $importHelper->checkConfiguration();
      }
      elseif ($action == 'createConfig') {
        $msg = $importHelper->createConfiguration();
      }
      elseif ($action == 'deleteQueue') {
        $msg = $importHelper->deleteQueue();
      }
      elseif ($action == 'clearStatus') {
        $msg = $importHelper->clearStatus();
      }
      elseif ($action == 'importOrgs') {
        $msg = $importHelper->import('tmp_uepal_orgdir', 'importCleanedOrgs');
      }
      elseif ($action == 'importHouseholds') {
        $msg = $importHelper->import('tmp_uepal_household', 'importHouseholds');
      }
      elseif ($action == 'importPersons') {
        $msg = $importHelper->import('tmp_uepal_pers', 'importPersons');
      }
      else {
        $msg = 'Error: Unknown action';
      }
    }
    catch (Exception $e) {
      $msg = 'Error: ' . $e->getMessage();
    }

    // process message (can be array or not)
    if (is_array($msg)) {
      if (count($msg) == 0) {
        $msg = '<p></p>';
      }
      elseif (count($msg) == 1) {
        $msg = '<p>' . $msg[0] . '</p>';
      }
      else {
        $msg = '<ul><li>' . implode('</li><li>', $msg) . '</li></ul>';
      }
    }
    else {
      $msg = '<p>' . $msg . '</p>';
    }

    $this->assign('action', $action);
    $this->assign('msg', $msg);

    parent::run();
  }

}
