from RobotRaconteur.Client import *
import numpy as np
import time
import os

from weldRRSensor import WeldRRSensor

def logger_response_cb(self, sub, value, ts):
    print("logger set sucessfully: ", str(value.success))
    print("logger message: ", str(value.message))

def main():

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

    # connect to the logger RR service
    logger_service = RRN.ConnectService('rr+tcp://localhost:12183?service=weldRRSensor_logger')
    logger_switch_wire = logger_service.logger_switch.Connect()
    logger_response_wire = logger_service.logger_switch_response.Connect()
    logger_response_wire.WireValueChanged += logger_response_cb
    sensor_switch_wire = logger_service.sensor_switch.Connect()

    # simulate welding
    for layer_i in range(3):
        # turn on the logger for this layer
        logger_switch_wire_v = RRN.NewStructure("experimental.rr_data_logger.logger_switch_struct")
        logger_switch_wire_v.logger_on_off = True
        logger_switch_wire_v.output_dir = os.path.join(os.getcwd(), 'layer_'+str(layer_i))
        logger_switch_wire.OutValue = logger_switch_wire_v
        # wait for the logger to respond that it is ready
        time.sleep(1)
        # set all sensors to ON for this layer
        sensor_switch_wire_v = RRN.NewStructure("experimental.rr_data_logger.sensor_switch_struct")
        sensor_switch_wire_v.robot_joints = True
        sensor_switch_wire_v.fronius = True
        sensor_switch_wire_v.current = True
        sensor_switch_wire_v.fujicam = True
        sensor_switch_wire_v.flir = True
        sensor_switch_wire_v.xiris = True
        sensor_switch_wire.OutValue = sensor_switch_wire_v

        # simulate welding for some time
        time.sleep(120) # welding

        # turn off the logger for this layer
        sensor_switch_wire_v = RRN.NewStructure("experimental.rr_data_logger.sensor_switch_struct")
        sensor_switch_wire_v.robot_joints = False
        sensor_switch_wire_v.fronius = False
        sensor_switch_wire_v.current = False
        sensor_switch_wire_v.fujicam = False
        sensor_switch_wire_v.flir = False
        sensor_switch_wire_v.xiris = False
        sensor_switch_wire.OutValue = sensor_switch_wire_v
        # turn off the logger
        logger_switch_wire_v.logger_on_off = False
        logger_switch_wire.OutValue = logger_switch_wire_v
        # wait for the logger to respond that it is done
        time.sleep(1)

        input("Press Enter to continue to the next layer...")

if __name__ == "__main__":
    main()