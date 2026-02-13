from RobotRaconteur.Client import *
from RobotRaconteurCompanion.Util.ImageUtil import ImageUtil
import argparse
import time, copy, os
import numpy as np
import traceback

from SQLiteStreamLogger import SQLiteStreamLogger

class WeldRRSensorLogger(object):
    def __init__(self,\
            datalogger_info: dict = None, \
            robot_service=None,\
            weld_service=None,\
            cam_service=None,\
            cam_2_service=None,\
            fujicam_service=None,\
            current_service=None) -> None:
        
        # sqlite data logger info
        self.datalogger_info = datalogger_info

        self.robot_service = robot_service
        self.weld_service = weld_service
        self.cam_service = cam_service
        self.cam_2_service = cam_2_service
        self.fujicam_service = fujicam_service
        self.current_service=current_service

        # initialize all callback flags to false to avoid reading the flags before the service is connected and the callback is set
        self.stop_all_sensors()

    def RRServiceObjectInit(self, ctx, service_path):

        ## robot service
        if self.robot_service:
            self.robot_joint_data_shape = (14,) # 2 6-joints robot + 1 2-joints positioner
            self.robot_joint_topic_name = "robot_joints"
            self.RR_robot_state = self.robot_service.SubscribeWire('robot_state')
            self.RR_robot_state.WireValueChanged += self.robot_state_cb
        
        ## weld service
        if self.weld_service:
            self.weld_log_data_shape = (4,) # voltage, current, feedrate, energy
            self.weld_log_topic_name = "fronius_welder"
            self.weld_obj = self.weld_service.GetDefaultClientWait(3)  # connect, timeout=30s
            self.welder_state_sub = self.weld_service.SubscribeWire("welder_state")
            self.welder_state_sub.WireValueChanged += self.weld_cb

        ## IR Camera Service - FLIR
        if self.cam_service:
            self.ir_image_consts = RRN.GetConstants('com.robotraconteur.image', self.cam_service)
            self.cam_pipe=self.cam_service.frame_stream.Connect(-1)
            # set the hdf5 save params
            rr_img = self.cam_service.capture_frame()
            self.ir_w = rr_img.image_info.width
            self.ir_h = rr_img.image_info.height
            self.ir_data_shape = (self.ir_h, self.ir_w) # image height and width
            self.ir_topic_name = "flir_ir_image"

            #Set the callback for new pipe packets
            self.cam_pipe.PacketReceivedEvent+=self.ir_cb

        ## IR Camera Service 2 - Xiris
        if self.cam_2_service:
            self.ir_2_image_consts = RRN.GetConstants("com.robotraconteur.imaging", self.cam_2_service)
            self.xiris_weldsdk_consts = RRN.GetConstants("experimental.xiris.weldsdk", self.cam_2_service)
            self.img_2_util = ImageUtil(client_obj=self.cam_2_service)
            self.cam_pipe_2=self.cam_2_service.frame_stream.Connect(-1)

            # set the hdf5 save params
            rr_img = self.cam_2_service.capture_frame()
            self.ir_2_w = rr_img.image_info.width
            self.ir_2_h = rr_img.image_info.height
            self.ir_2_data_shape = (self.ir_2_h, self.ir_2_w) # image height and width
            self.ir_2_topic_name = "xiris_ir_image"

            #Set the callback for new pipe packets
            self.cam_pipe_2.PacketReceivedEvent+=self.ir_2_cb
            try:
                self.cam_2_service.start_streaming()
            except:
                traceback.print_exc()
                print("error starting xiris stream")
        
        ## Scanner service - FujiCam
        if self.fujicam_service:
            self.fujicam_data_shape = (1016,3) # 1016 points per line, each point has (y,z,intensity)
            self.fujicam_topic_name = "fujicam_line_profile"
            # get service wire connection
            self.fujicam_obj = self.fujicam_service.GetDefaultClientWait(2)		#connect, timeout=2s
            self.fujicam_scan_wire=self.fujicam_service.SubscribeWire("lineProfile")
            self.fujicam_service.ClientConnectFailed += self.fujicam_connect_failed_handler
            # add wire value changed event handler
            self.fujicam_scan_wire.WireValueChanged += self.fujicam_value_changed_handler

        ## current service (current clamp sensor)
        if self.current_service:
            self.current_data_shape = (1,) # current clamp data only has 1 value: current
            self.current_topic_name = "current_clamp"
            self.current_state_sub = self.current_service.SubscribeWire("current")
            self.current_state_sub.WireValueChanged += self.current_cb

        ## calculate time offset
        self.t_offset = RRN.NowNodeTime().timestamp()-time.perf_counter()

        # register callback
        self.logger_switch.InValueChanged += self.logger_switch_cb
        self.sensor_switch.InValueChanged += self.sensor_switch_cb

    def logger_switch_cb(self, value, w, ts):
        
        try:
            out_dir = value.output_dir
            logger_on_off = value.logger_on_off
            logger_switch_response_wire_v = RRN.NewStructure("experimental.sensor_data_logger.logger_switch_response_struct")

            if logger_on_off:
                # if the client switch on the logger
                if hasattr(self, 'logger') and self.logger is not None:
                    # logger is already running, do not start a new one
                    msg = "Logger is already running. Please stop the logger before starting a new one."
                    logger_switch_response_wire_v.success = False
                    logger_switch_response_wire_v.message = msg   
                elif os.path.exists(out_dir):
                    msg = "path exisits"
                    # output directory already exists, do not start logger to avoid overwriting data
                    logger_switch_response_wire_v.success = False
                    logger_switch_response_wire_v.message = msg
                else:
                    msg = "start logger"
                    # start the logger
                    self.logger = SQLiteStreamLogger(
                        out_dir=out_dir,
                        base_name=self.datalogger_info['base_name'],
                        queue_max=self.datalogger_info['queue_max'],
                        batch_size=self.datalogger_info['batch_size'],
                        commit_period_s=self.datalogger_info['commit_period_s'],
                        drop_policy=self.datalogger_info['drop_policy'],
                        rollover_max_bytes=self.datalogger_info['rollover_max_bytes'],
                        checkpoint_on_rollover=self.datalogger_info['checkpoint_on_rollover'],
                        status_report_interval_s=self.datalogger_info['status_report_interval_s']
                    )
                    self.logger.start_logging()
                    msg = f"Logger started successfully. Output directory: {out_dir}"
                    logger_switch_response_wire_v.success = True
                    logger_switch_response_wire_v.message = msg
            else:
                # if the client switch off the logger
                self.stop_all_sensors() # stop all the sensor recording callbacks to avoid recording data when logger is stopped
                if hasattr(self, 'logger') and self.logger is not None:
                    self.logger.close()
                    msg = "Logger stopped successfully."
                    msg += "\nFinal stats:"
                    msg += str(self.logger.get_stats())
                    del self.logger
                    logger_switch_response_wire_v.success = True
                    logger_switch_response_wire_v.message = msg
                else:
                    msg = "Logger is not running."
                    logger_switch_response_wire_v.success = False
                    logger_switch_response_wire_v.message = msg
            
            print(msg)
            self.logger_switch_response.OutValue = logger_switch_response_wire_v
        
        except:
            traceback.print_exc()

        return

    def sensor_switch_cb(self, value, w , ts):

        try:
            msg = ''
            if not hasattr(self, 'logger') or self.logger is None:
                msg += "Logger is not running. Please switch ON the logger before switching ON the sensors."
            else:
                if value.robot_joints and self.robot_service:
                    msg += "Robot joints recording enabled."
                    self.logger.register_topic(self.robot_joint_topic_name, "float32", self.robot_joint_data_shape)
                    self.start_robot_cb=True
                elif self.robot_service:
                    msg += "Robot joints recording disabled."
                    self.start_robot_cb=False
                else:
                    msg += "Robot joint service not connected." 
                
                if value.fronius and self.weld_service:
                    msg += "Fronius welder recording enabled."
                    self.logger.register_topic(self.weld_log_topic_name, "float32", self.weld_log_data_shape)
                    self.start_weld_cb=True
                elif self.weld_service:
                    msg += "Fronius welder recording disabled."
                    self.start_weld_cb=False
                else:
                    msg += "Fronius welder service not connected."
                
                if value.current and self.current_service:
                    msg += "Current clamp recording enabled."
                    self.logger.register_topic(self.current_topic_name, "float32", self.current_data_shape)
                    self.start_current_cb=True
                elif self.current_service:
                    msg += "Current clamp recording disabled."
                    self.start_current_cb=False
                else:
                    msg += "Current clamp service not connected."
                
                if value.fujicam and self.fujicam_service:
                    msg += "FujiCam scanner recording enabled."
                    self.logger.register_topic(self.fujicam_topic_name, "float32", self.fujicam_data_shape)
                    self.start_fujicam_cb=True
                elif self.fujicam_service:
                    msg += "FujiCam scanner recording disabled."
                    self.start_fujicam_cb=False
                else:
                    msg += "FujiCam scanner service not connected."
                
                if value.flir and self.cam_service:
                    msg += "FLIR camera recording enabled."
                    self.logger.register_topic(self.ir_topic_name, "float32", self.ir_data_shape)
                    self.start_ir_cb=True
                elif self.cam_service:
                    msg += "FLIR camera recording disabled."
                    self.start_ir_cb=False
                else:
                    msg += "FLIR camera service not connected."
                
                if value.xiris and self.cam_2_service:
                    msg += "Xiris camera recording enabled."
                    self.logger.register_topic(self.ir_2_topic_name, "float32", self.ir_2_data_shape)
                    self.start_ir_2_cb=True
                elif self.cam_2_service:
                    msg += "Xiris camera recording disabled."
                    self.start_ir_2_cb=False
                else:
                    msg += "Xiris camera service not connected."
            
            print(msg)
            sensor_switch_response_wire_v = RRN.NewStructure("experimental.sensor_data_logger.sensor_switch_response_struct")
            sensor_switch_response_wire_v.robot_joints_status = self.start_robot_cb
            sensor_switch_response_wire_v.fronius_status = self.start_weld_cb
            sensor_switch_response_wire_v.current_status = self.start_current_cb
            sensor_switch_response_wire_v.fujicam_status = self.start_fujicam_cb
            sensor_switch_response_wire_v.flir_status = self.start_ir_cb
            sensor_switch_response_wire_v.xiris_status = self.start_ir_2_cb
            self.sensor_switch_response.OutValue = sensor_switch_response_wire_v
        except:
            traceback.print_exc()
    
    def start_all_sensors(self):

        if self.weld_service:
            self.start_weld_cb=True
        if self.cam_service:
            self.start_ir_cb=True
        if self.cam_2_service:
            self.start_ir_2_cb=True
        if self.fujicam_service:
            self.start_fujicam_cb=True
        if self.current_service:
            self.start_current_cb=True
        if self.robot_service:
            self.start_robot_cb = True

    def stop_all_sensors(self):

        print("Stopping all sensor recording...")
        self.start_robot_cb=False
        self.start_weld_cb=False
        self.start_ir_cb=False
        self.start_ir_2_cb=False
        self.start_fujicam_cb=False
        self.start_current_cb=False
    
    ##### robot state recording callbacks and functions #####
    def robot_state_cb(self, sub, value, ts):
        
        if self.start_robot_cb:
            timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
            self.logger.log_data(self.robot_joint_topic_name,
                                 np.array(value.joint_position).astype(np.float32),
                                 timestamp_ns=timestamp_ns)

    ##### welding and current recording callbacks and functions #####
    def weld_cb(self, sub, value, ts):

        if self.start_weld_cb:
            weld_data = np.array([value.welding_voltage, value.welding_current, value.wire_speed, value.welding_energy], dtype=np.float32)
            timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
            self.logger.log_data(self.weld_log_topic_name, weld_data, timestamp_ns=timestamp_ns)

    def current_cb(self, sub, value, ts):
        if self.start_current_cb:
            current_data = np.array([value], dtype=np.float32)
            timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
            self.logger.log_data(self.current_topic_name, current_data, timestamp_ns=timestamp_ns)

    ##### FLIR and Xiris camera callbacks and functions #####
    def ir_cb(self,pipe_ep):

        # Loop to get the newest frame
        while (pipe_ep.Available > 0):
            # Receive the packet
            rr_img = pipe_ep.ReceivePacket()
            if self.start_ir_cb:
                if rr_img.image_info.encoding == self.ir_image_consts["ImageEncoding"]["mono8"]:
                    # Simple uint8 image
                    mat = rr_img.data.reshape([rr_img.image_info.height, rr_img.image_info.width], order='C')
                elif rr_img.image_info.encoding == self.ir_image_consts["ImageEncoding"]["mono16"]:
                    data_u16 = np.array(rr_img.data.view(np.uint16))
                    mat = data_u16.reshape([rr_img.image_info.height, rr_img.image_info.width], order='C')

                ir_format = rr_img.image_info.extended["ir_format"].data

                if ir_format == "temperature_linear_10mK":
                    display_mat = (mat * 0.01) - 273.15
                elif ir_format == "temperature_linear_100mK":
                    display_mat = (mat * 0.1) - 273.15
                else:
                    display_mat = mat

                timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
                self.logger.log_data(self.ir_topic_name, 
                                     display_mat.astype(np.float32), timestamp_ns=timestamp_ns)

    def ir_2_cb(self,pipe_ep):
        # Loop to get the newest frame
        while (pipe_ep.Available > 0):
            # Receive the packet
            rr_img = pipe_ep.ReceivePacket()
            if self.start_ir_2_cb:
                # convert the packet to an image
                cv_img=self.img_2_util.image_to_array(rr_img)
                
                timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
                self.logger.log_data(self.ir_2_topic_name, 
                                     cv_img.astype(np.float32), timestamp_ns=timestamp_ns)
                
    ##### FujiCam scanner callbacks and functions #####
    def fujicam_connect_failed_handler(self, s, client_id, url, err):
        print ("Client connect failed: " + str(client_id.NodeID) + " url: " + str(url) + " error: " + str(err))

    def fujicam_value_changed_handler(self, con, wire_packet_value, ts):
        if not self.start_fujicam_cb:
            return
        
        y_data = wire_packet_value.Y_data
        z_data = wire_packet_value.Z_data
        I_data = wire_packet_value.I_data
        line_profile=np.hstack((y_data.reshape(-1,1),z_data.reshape(-1,1),I_data.reshape(-1,1)))

        # self.fujicam_line_profiles.append(line_profile)
        # self.fujicam_timestamps.append(time.perf_counter()+self.t_offset)
        timestamp_ns = int((time.perf_counter()+self.t_offset)*1e9)
        self.logger.log_data(self.fujicam_topic_name, 
                             line_profile.astype(np.float32), timestamp_ns=timestamp_ns)

def arg_parse():
    
    ap = argparse.ArgumentParser(description="WeldRRSensorLogger")
    ap.add_argument("--robot_joints", action='store_true', help="Log robot joint angles")
    ap.add_argument("--fronius", action='store_true', help="Log Fronius welder data")
    ap.add_argument("--current", action='store_true', help="Log current clamp data")
    ap.add_argument("--fujicam", action='store_true', help="Log FujiCam scanner data")
    ap.add_argument("--flir", action='store_true', help="Log FLIR camera data")
    ap.add_argument("--xiris", action='store_true', help="Log Xiris camera data")
    ap.add_argument("--robot_joints_url", type=str, default='rr+tcp://localhost:59945?service=robot', help="URL for robot joint state service")
    ap.add_argument("--fronius_url", type=str, default='rr+tcp://192.168.55.21:60823?service=welder', help="URL for Fronius welder service")
    ap.add_argument("--current_url", type=str, default='rr+tcp://192.168.55.21:12182?service=Current', help="URL for Current clamp service")
    ap.add_argument("--fujicam_url", type=str, default='rr+tcp://localhost:12181/?service=fujicam', help="URL for FujiCam scanner service")
    ap.add_argument("--flir_url", type=str, default='rr+tcp://192.168.55.10:60827/?service=camera', help="URL for FLIR camera service")
    ap.add_argument("--xiris_url", type=str, default='rr+tcp://127.0.0.1:59824/?service=camera', help="URL for Xiris camera service")
    ap.add_argument("--db_base_name", type=str, default="raw_db", help="Base name for output data files")
    ap.add_argument("--db_queue_max", type=int, default=4000, help="Max queue size for data logger")
    ap.add_argument("--db_batch_size", type=int, default=256, help="Batch size for data logger commits")
    ap.add_argument("--db_commit_period_s", type=float, default=0.05, help="Commit period (s) for data logger")
    ap.add_argument("--db_drop_policy", type=str, default="drop_oldest", choices=["drop_oldest", "drop_newest"], help="Drop policy for data logger when queue is full")
    ap.add_argument("--db_rollover_max_bytes", type=float, default=1, help="Max GB for data logger file rollover")
    ap.add_argument("--db_status_update_interval", type=float, default=10.0, help="Interval (s) for data logger status updates in console")

    args = ap.parse_args()
    return args

def main():

    # read in command line arguments
    args = arg_parse()

    # data logger information
    datalogger_info = {
        'base_name': args.db_base_name,
        'queue_max': args.db_queue_max,
        'batch_size': args.db_batch_size,
        'commit_period_s': args.db_commit_period_s,
        'drop_policy': args.db_drop_policy,
        'rollover_max_bytes': int(args.db_rollover_max_bytes * 1024 * 1024 * 1024),  # N GiB segments (demo)
        'checkpoint_on_rollover': True,
        'status_report_interval_s': args.db_status_update_interval
    }

    with RR.ServerNodeSetup("experimental.sensor_data_logger", 12182):
        # robot joint angles
        RR_robot_sub = None
        if args.robot_joints:
            robot_joints_url = args.robot_joints_url
            RR_robot_sub=RRN.SubscribeService(robot_joints_url)
        # fronius welder log
        fronius_sub = None
        if args.fronius:
            fronius_url = args.fronius_url
            fronius_sub=RRN.SubscribeService(fronius_url)
        # Current clamp log
        current_ser = None
        if args.current:
            current_url = args.current_url
            current_ser=RRN.SubscribeService(current_url)
        # fujicam scanner
        fujicam_ser = None
        if args.fujicam:
            fujicam_url = args.fujicam_url
            fujicam_ser=RRN.SubscribeService(fujicam_url)
        # FLIR
        flir_ser = None
        if args.flir:
            flir_url = args.flir_url
            flir_ser=RRN.ConnectService(flir_url)
        # Xiris
        xiris_ser = None
        if args.xiris:
            xiris_url = args.xiris_url
            xiris_ser=RRN.ConnectService(xiris_url)

        # Register the service type
        # RRN.RegisterServiceType(sensor_data_logger)
        RRN.RegisterServiceTypeFromFile("robdef/experimental.sensor_data_logger")

        weld_logger = WeldRRSensorLogger(\
            datalogger_info=datalogger_info,\
            robot_service=RR_robot_sub,\
            weld_service=fronius_sub,\
            cam_service=flir_ser,\
            cam_2_service=xiris_ser,\
            fujicam_service=fujicam_ser,\
            current_service=current_ser)
        
        # Register the service
        RRN.RegisterService("weldRRSensor_logger", "experimental.sensor_data_logger.sensor_data_logger_obj", weld_logger)
        print("WeldRRSensor Logger Service started...")
        print("Data logger base name: ", args.db_base_name)
        print("DB queue max size: ", args.db_queue_max)
        print("DB batch size: ", args.db_batch_size)
        print("DB commit period (s): ", args.db_commit_period_s)
        print("DB drop policy: ", args.db_drop_policy)
        print("DB rollover max size (GB): ", args.db_rollover_max_bytes)
        print("Services:")
        print("Robot joints: ", args.robot_joints)
        print("Fronius welder: ", args.fronius)
        print("Current clamp: ", args.current)
        print("Fujicam scanner: ", args.fujicam)
        print("FLIR camera: ", args.flir)
        print("Xiris camera: ", args.xiris)
        
        input("Press Enter to exit...\n")
        if hasattr(weld_logger, 'logger') and weld_logger.logger is not None:
            weld_logger.stop_all_sensors() # stop all the sensor recording callbacks to avoid recording data when logger is stopped
            weld_logger.logger.close()

if __name__ == "__main__":
    main()
