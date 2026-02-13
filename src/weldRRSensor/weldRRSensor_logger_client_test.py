from RobotRaconteur.Client import *
import numpy as np
import time
import os
import threading

from weldRRSensor import WeldRRSensor




class LoggerHelper(object):
    def __init__(self, logger_rr_url = 'rr+tcp://localhost:12182?service=weldRRSensor_logger'):
        # connect to the logger RR service
        self.logger_service = RRN.ConnectService(logger_rr_url)
        self.logger_switch_wire = self.logger_service.logger_switch.Connect()
        self.logger_response_wire = self.logger_service.logger_switch_response.Connect()
        self.logger_response_wire.WireValueChanged += self.logger_response_cb
        self.sensor_switch_wire = self.logger_service.sensor_switch.Connect()
        self.sensor_response_wire = self.logger_service.sensor_switch_response.Connect()
        self.sensor_response_wire.WireValueChanged += self.logger_sensor_response_cb

        # private
        # logger thread event
        self._logger_ready_evt = threading.Event()
        self._logger_lock = threading.Lock()
        self._logger_last_resp = None
        self._sensor_ready_evt = threading.Event()
        self._sensor_lock = threading.Lock()
        self._sensor_last_resp = None
    
    def logger_response_cb(self, sub, value, ts):

        # Callback thread -> just record and signal
        with self._logger_lock:
            self._logger_last_resp = (value, ts)
        self._logger_ready_evt.set()

    def logger_switch(self, switch_on_off, output_dir=None, logger_response_timeout=10):

        if (not switch_on_off) and (output_dir is None):
            print("Warning: switch off needs output directory")
            return False, ''

        # turn on the logger for this layer
        logger_switch_wire_v = RRN.NewStructure("experimental.sensor_data_logger.logger_switch_struct")
        logger_switch_wire_v.logger_on_off = True
        logger_switch_wire_v.output_dir = output_dir
        self.logger_switch_wire.OutValue = logger_switch_wire_v # send the wire value

        """Wait for the next logger response callback."""
        ok = self._logger_ready_evt.wait(timeout=logger_response_timeout)

        if not ok:
            # raise TimeoutError(f"Logger did not respond within {logger_response_timeout:.1f}s")
            print(f"Warning: Logger did not respond within {logger_response_timeout:.1f}s")
            return False, ''

        with self._logger_lock:
            value, ts = self._logger_last_resp
            print("logger set sucessfully: ", str(value.success))
            print("logger message: \n", str(value.message))

        # Reset for next use (important!)
        self._logger_ready_evt.clear()

        return value.success, value.message
    
    def logger_sensor_response_cb(self, sub, value, ts):

        # Callback thread -> just record and signal
        with self._sensor_lock:
            self._sensor_last_resp = (value, ts)
        self._sensor_ready_evt.set()
    
    def loggger_sensor_switch(self,robot_joints_switch=False,fronius_switch=False,current_switch=False,
                              fujicam_switch=False,flir_switch=False,xiris_switch=False,logger_response_timeout=10.):

        sensor_switch_wire_v = RRN.NewStructure("experimental.sensor_data_logger.sensor_switch_struct")
        sensor_switch_wire_v.robot_joints = robot_joints_switch
        sensor_switch_wire_v.fronius = fronius_switch
        sensor_switch_wire_v.current = current_switch
        sensor_switch_wire_v.fujicam = fujicam_switch
        sensor_switch_wire_v.flir = flir_switch
        sensor_switch_wire_v.xiris = xiris_switch
        self.sensor_switch_wire.OutValue = sensor_switch_wire_v

        """Wait for the next logger sensor response callback."""
        ok = self._logger_ready_evt.wait(timeout=logger_response_timeout)

        if not ok:
            # raise TimeoutError(f"Logger did not respond within {logger_response_timeout:.1f}s")
            print(f"Warning: Logger did not respond within {logger_response_timeout:.1f}s")
            return (False,False,False,False,False,False), ''

        with self._sensor_lock:
            value, ts = self._sensor_last_resp
            robot_joints_status = value.robot_joints_status
            fronius_status = value.fronius_status
            current_status = value.current_status
            fujicam_status = value.fujicam_status
            flir_status = value.flir_status
            xiris_status = value.xiris_status
            sensor_status = (robot_joints_status,fronius_status,current_status,fujicam_status,\
                             flir_status,xiris_status)

        # Reset for next use (important!)
        self._logger_ready_evt.clear()

        return sensor_status, value.message
        
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
    logger = LoggerHelper(logger_rr_url=logger_url)

    # simulate welding
    for layer_i in range(3):
        print("Starting the layer",layer_i)
        logger_start_success, logger_info = logger.logger_switch(
            switch_on_off=True, output_dir=os.path.join(os.getcwd(), 'layer_'+str(layer_i))
        )
        print("Turn on the sensors")
        # set all sensors to ON for this layer
        sensor_status, logger_info = logger.loggger_sensor_switch(
            robot_joints_switch=True, fronius_switch=True, current_switch=True,
            fujicam_switch=True, flir_switch=True, xiris_switch=True
        )

        # simulate welding for some time
        time.sleep(20) # welding

        # turn off the logger for this layer
        sensor_status, logger_info = logger.loggger_sensor_switch(
            robot_joints_switch=False, fronius_switch=False, current_switch=False,
            fujicam_switch=False, flir_switch=False, xiris_switch=False
        )
        # turn off the logger
        logger_start_success, logger_info = logger.logger_switch(
            switch_on_off=False
        )

        input("Press Enter to continue to the next layer...")

if __name__ == "__main__":
    main()