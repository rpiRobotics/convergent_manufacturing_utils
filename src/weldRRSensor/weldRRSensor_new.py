from RobotRaconteur.Client import *
from RobotRaconteurCompanion.Util.ImageUtil import ImageUtil
import time, copy, threading
import numpy as np
from matplotlib import pyplot as plt
import sys

class WeldRRSensor(object):
    def __init__(self,
            robot_service=None,\
            robot_joint_buffer = 250,\
            weld_service = None,\
            weldlog_msg_buffer = 20, \
            cam_service = None,\
            flir_focus_pos = 1900,\
            flir_object_distance = 0.4,\
            flir_msg_buffer = 100,\
            cam_2_service = None,\
            xiris_emissivity = 1.0, \
            xiris_msg_buffer = 100, \
            fujicam_service = None,\
            fujicam_msg_buffer = 500, \
            current_service = None, \
            current_msg_buffer = 2000) -> None:
        
        self.msg_type = np.float64

        ## calculate time offset
        self.t_offset = RRN.NowNodeTime().timestamp()-time.perf_counter()

        ## robot joints service
        self.robot_service=robot_service
        if self.robot_service:
            self.robot_joint_data_shape = (14,) # 2 6-joints robot + 1 2-joints positioner
            # weld log message buffer
            self.robot_joint_frames = np.empty((robot_joint_buffer,14), dtype=self.msg_type)
            self.robot_joint_ts = np.empty((robot_joint_buffer,), dtype=self.msg_type)
            self._robot_joint_write_idx = 0
            self._robot_joint_count = 0
            self._robot_joint_lock = threading.Lock()

            self.RR_robot_state = self.robot_service
            self.RR_robot_state.WireValueChanged += self.robot_state_cb

        ## weld service
        self.weld_service=weld_service
        if weld_service:
            # weld log message buffer
            self.weldlog_frames = np.empty((weldlog_msg_buffer,4), dtype=self.msg_type)
            self.weldlog_ts = np.empty((weldlog_msg_buffer,), dtype=self.msg_type)
            self._weldlog_write_idx = 0
            self._weldlog_count = 0
            self._weldlog_lock = threading.Lock()

            self.weld_obj = self.weld_service.GetDefaultClientWait(3)  # connect, timeout=30s
            self.welder_state_sub = self.weld_service.SubscribeWire("welder_state")
            self.start_weld_cb = False
            self.welder_state_sub.WireValueChanged += self.weld_cb

        ## IR Camera Service - FLIR
        self.cam_ser=cam_service
        if cam_service:
            self.ir_image_consts = RRN.GetConstants('com.robotraconteur.image', self.cam_ser)

            self.cam_ser.setf_param("focus_pos", RR.VarValue(int(flir_focus_pos),"int32"))
            self.cam_ser.setf_param("object_distance", RR.VarValue(flir_object_distance,"double"))
            self.cam_ser.setf_param("reflected_temperature", RR.VarValue(291.15,"double"))
            self.cam_ser.setf_param("atmospheric_temperature", RR.VarValue(293.15,"double"))
            self.cam_ser.setf_param("relative_humidity", RR.VarValue(50,"double"))
            self.cam_ser.setf_param("ext_optics_temperature", RR.VarValue(293.15,"double"))
            self.cam_ser.setf_param("ext_optics_transmission", RR.VarValue(0.99,"double"))
            self.cam_ser.setf_param("current_case", RR.VarValue(2,"int32"))
            self.cam_ser.setf_param("ir_format", RR.VarValue("radiometric","string"))
            self.cam_ser.setf_param("object_emissivity", RR.VarValue(0.13,"double"))
            self.cam_ser.setf_param("scale_limit_low", RR.VarValue(293.15,"double"))
            self.cam_ser.setf_param("scale_limit_upper", RR.VarValue(5000,"double"))

            self.cam_pipe=self.cam_ser.frame_stream.Connect(-1)
            
            #Set the callback for new pipe packets
            self.cam_pipe.PacketReceivedEvent+=self.ir_cb

            # set the hdf5 save params
            rr_img = cam_service.capture_frame()
            self.ir_w = rr_img.image_info.width
            self.ir_h = rr_img.image_info.height

            # FLIR camera message buffer
            self.ir_frames = np.empty((flir_msg_buffer, self.ir_h, self.ir_w), dtype=self.msg_type)
            self.ir_ts = np.empty((flir_msg_buffer,), dtype=self.msg_type)
            self._ir_write_idx = 0
            self._ir_count = 0
            self._ir_lock = threading.Lock()
            try:
                self.cam_ser.start_streaming()
            except:
                pass

        ## IR Camera Service 2 - Xiris
        self.cam_2_ser=cam_2_service
        if cam_2_service:
            self.ir_2_image_consts = RRN.GetConstants("com.robotraconteur.imaging", self.cam_2_ser)
            self.xiris_weldsdk_consts = RRN.GetConstants("experimental.xiris.weldsdk", self.cam_2_ser)

            self.cam_2_ser.setf_param("camera_operating_mode", RR.VarValue("thermography", "string"))
            self.cam_2_ser.trigger_mode = self.ir_2_image_consts["TriggerMode"]["external"]
            self.cam_2_ser.trigger_polarity = self.xiris_weldsdk_consts["TriggerPolarities"]["positive"]
            self.cam_2_ser.trigger_delay = 250

            # set emissivity map
            em_val = xiris_emissivity
            em_map = np.ones([512, 640], dtype=np.float64)
            em_map = em_val*em_map
            image_type = RRN.GetStructureType("com.robotraconteur.image.Image", self.cam_2_ser)
            image_info_type = RRN.GetStructureType("com.robotraconteur.image.ImageInfo", self.cam_2_ser)
            image_const = RRN.GetConstants("com.robotraconteur.image", self.cam_2_ser)

            rr_image = image_type()
            rr_image_info = image_info_type()
            rr_image.image_info = rr_image_info
            rr_image_info.width = 640
            rr_image_info.height = 512
            rr_image_info.encoding = image_const["ImageEncoding"]["mono_f64"]
            rr_image.data = em_map.flatten(order="C").view(dtype=np.uint8).copy()
            self.cam_2_ser.setf_emissivity_map(rr_image)

            self.img_2_util = ImageUtil(client_obj=self.cam_2_ser)
            self.cam_pipe_2=self.cam_2_ser.frame_stream.Connect(-1)

            #Set the callback for new pipe packets
            self.start_ir_2_cb = False
            self.cam_pipe_2.PacketReceivedEvent+=self.ir_2_cb
            
            rr_img = cam_2_service.capture_frame()
            self.ir_2_w = rr_img.image_info.width
            self.ir_2_h = rr_img.image_info.height

            # Xiris camera message buffer
            self.ir_2_frames = np.empty((xiris_msg_buffer, self.ir_2_h, self.ir_2_w), dtype=self.msg_type)
            self.ir_2_ts = np.empty((xiris_msg_buffer,), dtype=self.msg_type)
            self._ir_2_write_idx = 0
            self._ir_2_count = 0
            self._ir_2_lock = threading.Lock()
            try:
                self.cam_2_ser.start_streaming()
            except:
                print("error starting xiris stream")
        
        ## Scanner service - FujiCam
        self.fujicam_service=fujicam_service
        if fujicam_service:

            # Fujicam message buffer
            self.fujicam_points = 1016
            self.fujicam_data_shape = (self.fujicam_points,2)
            self.fujicam_frames = np.empty((fujicam_msg_buffer,self.fujicam_points, 2), dtype=self.msg_type)
            self.fujicam_ts = np.empty((fujicam_msg_buffer,), dtype=self.msg_type)
            self._fujicam_write_idx = 0
            self._fujicam_count = 0
            self._fujicam_lock = threading.Lock()
            
            # get service wire connection
            self.fujicam_obj = self.fujicam_service.GetDefaultClientWait(2)		#connect, timeout=2s
            self.fujicam_scan_wire=self.fujicam_service.SubscribeWire("lineProfile")
            self.fujicam_service.ClientConnectFailed += self.fujicam_connect_failed_handler
            # add wire value changed event handler
            self.fujicam_scan_wire.WireValueChanged += self.fujicam_value_changed_handler

        ## current service (current clamp sensor)
        self.current_service=current_service
        if current_service:
            # Current message buffer
            self.current_frames = np.empty((current_msg_buffer,), dtype=self.msg_type)
            self.current_ts = np.empty((current_msg_buffer,), dtype=self.msg_type)
            self._current_write_idx = 0
            self._current_count = 0
            self._current_lock = threading.Lock()

            self.current_state_sub = self.current_service.SubscribeWire("current")
            self.start_current_cb = False
            self.current_state_sub.WireValueChanged += self.current_cb

    def _get_recent_msgs(self, k, count, write_idx, msg_buffer, ts_buffer):

        if count <= 0 or k <=0 :
            return None, None
        
        k = min(k, count)
        N = msg_buffer.shape[0]
        
        if k == 1:
            idx = (write_idx - 1) % N
            return ts_buffer[idx].item(), msg_buffer[idx].copy()
        
        end = write_idx
        start = (end - k) % N

        frames = np.empty((k, *msg_buffer.shape[1:]), dtype=msg_buffer.dtype)
        ts = np.empty((k,), dtype=ts_buffer.dtype)

        first_part = N - start
        if first_part >= k:
            frames[:] = msg_buffer[start:start+k]
            ts[:] = ts_buffer[start:start+k]
        else:
            frames[:first_part] = msg_buffer[start:]
            frames[first_part:] = msg_buffer[:k-first_part]
            ts[:first_part] = ts_buffer[start:]
            ts[first_part:] = ts_buffer[:k-first_part]

        return ts, frames
    
    def robot_state_cb(self, sub, value, ts):
        
        # add to preallocate queue
        ts_offset = time.perf_counter()+self.t_offset
        if self._robot_joint_lock.acquire(blocking=False):
            try:
                i = self._robot_joint_write_idx
                self.robot_joint_frames[i] = np.array(value.joint_position).astype(self.msg_type)  # copy into ring buffer
                self.robot_joint_ts[i] = ts_offset

                self._robot_joint_write_idx = (i + 1) % self.robot_joint_frames.shape[0]
                self._robot_joint_count = min(self._robot_joint_count + 1, self.robot_joint_frames.shape[0])
            finally:
                self._robot_joint_lock.release()
        else:
            pass  # drop
    
    def get_recent_robot_joints(self, k=1):

        if k==0 or not self._robot_joint_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._robot_joint_count, self._robot_joint_write_idx,
                                        self.robot_joint_frames, self.robot_joint_ts)
        finally:
            self._robot_joint_lock.release()
        return ts, frames

    
    def weld_cb(self, sub, value, ts):
        
        # add to preallocate queue
        ts_offset = time.perf_counter()+self.t_offset
        if self._weldlog_lock.acquire(blocking=False):
            try:
                i = self._weldlog_write_idx
                self.weldlog_frames[i] = np.array([value.welding_voltage,value.welding_current,value.wire_speed,value.welding_energy])  # copy into ring buffer
                self.weldlog_ts[i] = ts_offset

                self._weldlog_write_idx = (i + 1) % self.weldlog_frames.shape[0]
                self._weldlog_count = min(self._weldlog_count + 1, self.weldlog_frames.shape[0])
            finally:
                self._weldlog_lock.release()
        else:
            pass  # drop

    def get_recent_weld_msg(self, k=1):

        if k==0 or not self._weldlog_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._weldlog_count, self._weldlog_write_idx,
                                        self.weldlog_frames, self.weldlog_ts)
        finally:
            self._weldlog_lock.release()
        return ts, frames

    def current_cb(self, sub, value, ts):
        
        # add to preallocate queue
        ts_offset = time.perf_counter()+self.t_offset
        if self._current_lock.acquire(blocking=False):
            try:
                i = self._current_write_idx
                self.current_frames[i] = value  # copy into ring buffer
                self.current_ts[i] = ts_offset

                self._current_write_idx = (i + 1) % self.current_frames.shape[0]
                self._current_count = min(self._current_count + 1, self.current_frames.shape[0])
            finally:
                self._current_lock.release()
        else:
            pass  # drop
    
    def get_recent_current_msg(self, k=1):

        if k==0 or not self._current_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._current_count, self._current_write_idx,
                                        self.current_frames, self.current_ts)
        finally:
            self._current_lock.release()
        return ts, frames

    ##### FLIR and Xiris camera callbacks and functions #####

    def ir_cb(self,pipe_ep):

        # Loop to get the newest frame
        while (pipe_ep.Available > 0):
            # Receive the packet
            rr_img = pipe_ep.ReceivePacket()
            ts_offset = time.perf_counter() + self.t_offset

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

            # ... build display_mat as float32 array ...
            display_mat = display_mat.astype(self.msg_type)
            # add to preallocate queue
            if self._ir_lock.acquire(blocking=False):
                try:
                    i = self._ir_write_idx
                    self.ir_frames[i, :, :] = display_mat  # copy into ring buffer
                    self.ir_ts[i] = ts_offset

                    self._ir_write_idx = (i + 1) % self.ir_frames.shape[0]
                    self._ir_count = min(self._ir_count + 1, self.ir_frames.shape[0])
                finally:
                    self._ir_lock.release()
            else:
                pass  # drop

    def get_recent_ir_msg(self, k=1):

        if k==0 or not self._ir_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._ir_count, self._ir_write_idx,
                                        self.ir_frames, self.ir_ts)
        finally:
            self._ir_lock.release()
        return ts, frames

    def ir_2_cb(self,pipe_ep):
        # Loop to get the newest frame
        while (pipe_ep.Available > 0):
            # Receive the packet
            rr_img = pipe_ep.ReceivePacket()
            ts_offset = time.perf_counter() + self.t_offset

            # convert the packet to an image
            cv_img=self.img_2_util.image_to_array(rr_img)

            # ... build display_mat as float32 array ...
            cv_img = cv_img.astype(self.msg_type)
            
            # add to preallocate queue
            if self._ir_2_lock.acquire(blocking=False):
                try:
                    i = self._ir_2_write_idx
                    self.ir_2_frames[i, :, :] = cv_img  # copy into ring buffer
                    self.ir_2_ts[i] = ts_offset

                    self._ir_2_write_idx = (i + 1) % self.ir_2_frames.shape[0]
                    self._ir_2_count = min(self._ir_2_count + 1, self.ir_2_frames.shape[0])
                finally:
                    self._ir_2_lock.release()
            else:
                pass  # drop
    
    def get_recent_ir_2_msg(self, k=1):

        if k==0 or not self._ir_2_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._ir_2_count, self._ir_2_write_idx,
                                        self.ir_2_frames, self.ir_2_ts)
        finally:
            self._ir_2_lock.release()
        return ts, frames
                
    ##### FujiCam scanner callbacks and functions #####

    def fujicam_connect_failed_handler(self, s, client_id, url, err):
        print ("Client connect failed: " + str(client_id.NodeID) + " url: " + str(url) + " error: " + str(err))

    def fujicam_value_changed_handler(self, con, wire_packet_value, ts):

        ts_offset = time.perf_counter() + self.t_offset
        line_profile = np.zeros(self.fujicam_data_shape, dtype=self.msg_type)
        line_profile[:,0] = wire_packet_value.Y_data
        line_profile[:,1] = wire_packet_value.Z_data

        # add to preallocate queue
        if self._fujicam_lock.acquire(blocking=False):
            try:
                i = self._fujicam_write_idx
                self.fujicam_frames[i, :, :] = line_profile  # copy into ring buffer
                self.fujicam_ts[i] = ts_offset

                self._fujicam_write_idx = (i + 1) % self.fujicam_frames.shape[0]
                self._fujicam_count = min(self._fujicam_count + 1, self.fujicam_frames.shape[0])
            finally:
                self._fujicam_lock.release()
        else:
            pass  # drop

    def get_recent_fujicam_msg(self, k=1):

        if k==0 or not self._fujicam_lock.acquire(blocking=False):
            return None, None
        try:
            ts, frames = self._get_recent_msgs(k, self._fujicam_count, self._fujicam_write_idx,
                                        self.fujicam_frames, self.fujicam_ts)
        finally:
            self._fujicam_lock.release()
        return ts, frames
