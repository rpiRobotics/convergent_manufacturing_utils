from RobotRaconteur.Client import *
import numpy as np
import time
import os
import threading

from weldRRSensor_new import WeldRRSensor
from weldRRSensor_logger import LoggerClientHelper
        
def main():

    RRN.RegisterServiceTypeFromFile("robdef/experimental.sensor_data_logger")

    RR_robot_sub = RRN.SubscribeService('rr+tcp://localhost:59945?service=robot')
    fronius_sub=RRN.SubscribeService('rr+tcp://192.168.55.21:60823?service=welder')
    current_ser=RRN.SubscribeService('rr+tcp://192.168.55.21:12182?service=Current')
    cam_ser=RRN.ConnectService('rr+tcp://192.168.55.10:60827/?service=camera')
    xiris_service=RRN.ConnectService('rr+tcp://127.0.0.1:59824/?service=camera')
    fujicam_ser=RRN.SubscribeService('rr+tcp://localhost:12181/?service=fujicam')

    # start the sensor but dont collect data using this
    rr_sensors = WeldRRSensor(weld_service=fronius_sub,
                cam_service=cam_ser,flir_focus_pos = 1900,flir_object_distance = 0.479,\
                cam_2_service=xiris_service,current_service=current_ser,
                fujicam_service=fujicam_ser)
    
    logger_url = 'rr+tcp://localhost:12182?service=weldRRSensor_logger'
    logger = LoggerClientHelper(logger_rr_url=logger_url)

    # simulate welding
    for layer_i in range(3):
        print("Starting the layer",layer_i)
        logger_start_success, logger_info = logger.logger_switch(
            switch_on_off=True, 
            robot_joints_register=True, fronius_register=True, current_register=True,
            fujicam_register=True, flir_register=True, xiris_register=True,
            output_dir=os.path.join(os.getcwd(), 'layer_'+str(layer_i))
        )
        # input()
        assert logger_start_success, 'Logger does not successfully start because of problems.'
        print("Turn on the sensors")
        # set all sensors to ON for this layer
        sensor_status, logger_info = logger.loggger_sensor_switch(
            robot_joints_switch=True, fronius_switch=True, current_switch=True,
            fujicam_switch=True, flir_switch=True, xiris_switch=True
        )
        print("Turn on the sensors success")

        # simulate welding for some time
        time.sleep(20) # welding

        # turn off the logger for this layer
        print("Turning off the sensor")
        sensor_status, logger_info = logger.loggger_sensor_switch(
            robot_joints_switch=False, fronius_switch=False, current_switch=False,
            fujicam_switch=False, flir_switch=False, xiris_switch=False
        )
        print("Turing off the logger and save")
        # turn off the logger
        logger_start_success, logger_info = logger.logger_switch(
            switch_on_off=False
        )

        input("Press Enter to continue to the next layer...")

if __name__ == "__main__":
    main()