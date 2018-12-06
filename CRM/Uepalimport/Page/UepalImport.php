<?php
use CRM_Uepalimport_ExtensionUtil as E;

class CRM_Uepalimport_Page_UepalImport extends CRM_Core_Page {

  /**
   * This is the import main page.
   * The corresponding .tpl file contains the menu.
   */
  public function run() {
    CRM_Utils_System::setTitle(E::ts('Uepal Import'));

    parent::run();
  }

}
