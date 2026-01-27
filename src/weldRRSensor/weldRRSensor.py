from RobotRaconteur.Client import *
from RobotRaconteurCompanion.Util.ImageUtil import ImageUtil
import time, copy, pickle, wave
import numpy as np
from matplotlib import pyplot as plt
from pathlib import Path
import h5py

class WeldRRSensor(object):
    def __init__(self,weld_service=None,\
            cam_service=None,\
            flir_focus_pos = 1900,\
            flir_object_distance = 0.4,\
            cam_2_service=None,\
            fujicam_service=None,\
            microphone_service=None,\
            current_service=None) -> None:

        ## weld service
        self.weld_service=weld_service
        if weld_service:
            self.weld_obj = self.weld_service.GetDefaultClientWait(3)  # connect, timeout=30s
            self.welder_state_sub = self.weld_service.SubscribeWire("welder_state")
            self.start_weld_cb = False
            self.clean_weld_record()
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
            self.start_ir_cb = False
            self.cam_pipe.PacketReceivedEvent+=self.ir_cb

            # set the hdf5 save params
            rr_img = cam_service.capture_frame()
            self.ir_w = rr_img.image_info.width
            self.ir_h = rr_img.image_info.height
            self.ir_chunk_shape = (1, self.ir_h, self.ir_w)
            self.ir_compression_alg = 'gzip'

            try:
                self.cam_ser.start_streaming()
            except:
                pass

            self.clean_ir_record()

        ## IR Camera Service 2 - Xiris
        self.cam_2_ser=cam_2_service
        if cam_2_service:
            self.ir_2_image_consts = RRN.GetConstants("com.robotraconteur.imaging", self.cam_2_ser)
            self.xiris_weldsdk_consts = RRN.GetConstants("experimental.xiris.weldsdk", self.cam_2_ser)

            self.cam_2_ser.setf_param("camera_operating_mode", RR.VarValue("thermography", "string"))
            self.cam_2_ser.trigger_mode = self.ir_2_image_consts["TriggerMode"]["external"]
            self.cam_2_ser.trigger_polarity = self.xiris_weldsdk_consts["TriggerPolarities"]["positive"]
            self.cam_2_ser.trigger_delay = 250

            self.img_2_util = ImageUtil(client_obj=self.cam_2_ser)
            self.cam_pipe_2=self.cam_2_ser.frame_stream.Connect(-1)
            #Set the callback for new pipe packets
            self.start_ir_2_cb = False
            self.cam_pipe_2.PacketReceivedEvent+=self.ir_2_cb
            
            # set the hdf5 save params
            rr_img = cam_2_service.capture_frame()
            self.ir_2_w = rr_img.image_info.width
            self.ir_2_h = rr_img.image_info.height
            self.ir_2_chunk_shape = (1, self.ir_2_h, self.ir_2_w)
            self.ir_2_compression_alg = 'gzip'

            try:
                self.cam_2_ser.start_streaming()
            except:
                print("error starting xiris stream")
            self.clean_ir_2_record()
        
        ## Scanner service - FujiCam
        self.fujicam_service=fujicam_service
        if fujicam_service:
            self.start_fujicam_cb=False
            # get service wire connection
            self.fujicam_obj = self.fujicam_service.GetDefaultClientWait(2)		#connect, timeout=2s
            self.fujicam_scan_wire=self.fujicam_service.SubscribeWire("lineProfile")
            self.fujicam_service.ClientConnectFailed += self.fujicam_connect_failed_handler
            # add wire value changed event handler
            self.fujicam_scan_wire.WireValueChanged += self.fujicam_value_changed_handler

            # Compression for hdf5
            self.fujicam_compression_alg='gzip'

            # initialize recording storage
            self.clean_fujicam_record()

        ## microphone service
        self.mic_service=microphone_service
        if microphone_service:
            self.mic_samplerate = 44100
            self.mic_channels = 1
            self.mic_service=microphone_service
            self.mic_pipe = self.mic_service.microphone_stream.Connect(-1)
            self.clean_mic_record()
            self.start_mic_cb=False
            self.mic_pipe.PacketReceivedEvent+=self.microphone_cb

        ## current service (current clamp sensor)
        self.current_service=current_service
        if current_service:
            self.current_state_sub = self.current_service.SubscribeWire("current")
            self.start_current_cb = False
            self.clean_current_record()
            self.current_state_sub.WireValueChanged += self.current_cb

        ## calculate time offset
        self.t_offset = RRN.NowNodeTime().timestamp()-time.perf_counter()

    def start_all_sensors(self):

        if self.weld_service:
            self.clean_weld_record()
            self.start_weld_cb=True
        if self.cam_ser:
            self.clean_ir_record()
            self.start_ir_cb=True
        if self.cam_2_ser:
            self.clean_ir_2_record()
            self.start_ir_2_cb=True
        if self.fujicam_service:
            self.clean_fujicam_record()
            self.start_fujicam_cb=True
        if self.mic_service:
            self.clean_mic_record()
            self.start_mic_cb=True
        if self.current_service:
            self.clean_current_record()
            self.start_current_cb=True

    def clear_all_sensors(self):
        if self.weld_service:
            self.clean_weld_record()
        if self.cam_ser:
            self.clean_ir_record()
        if self.cam_2_ser:
            self.clean_ir_2_record()
        if self.fujicam_service:
            self.clean_fujicam_record()
        if self.mic_service:
            self.clean_mic_record()
        if self.current_service:
            self.clean_current_record()

    def stop_all_sensors(self):

        self.start_weld_cb=False
        self.start_ir_cb=False
        self.start_ir_2_cb=False
        self.start_fujicam_cb=False
        self.start_mic_cb=False
        self.start_current_cb=False

    def save_all_sensors(self,filedir):

        if self.weld_service:
            self.save_weld_file(filedir)
        if self.cam_ser:
            self.save_ir_file(filedir)
        if self.cam_2_ser:
            self.save_ir_2_file(filedir)
        if self.fujicam_service:
            self.save_fujicam_file(filedir)
        if self.mic_service:
            self.save_mic_file(filedir)
        if self.current_service:
            self.save_current_file(filedir)

    def test_all_sensors(self,t=3):

        self.start_all_sensors()
        time.sleep(t)
        self.stop_all_sensors()

        if self.cam_ser:
            fig = plt.figure(1)
            sleep_t=float(3./len(self.ir_recording))
            for r in self.ir_recording:
                plt.imshow(r, cmap='inferno', aspect='auto')
                plt.colorbar(format='%.2f')
                plt.pause(sleep_t)
                plt.clf()
        if self.cam_2_ser:
            fig = plt.figure(1)
            sleep_t=float(3./len(self.ir_2_recording))
            for r in self.ir_2_recording:
                plt.imshow(r, cmap='inferno', aspect='auto')
                plt.colorbar(format='%.2f')
                plt.pause(sleep_t)
                plt.clf()
        if self.mic_service:
            first_channel = np.concatenate(self.audio_recording)
            first_channel_int16=(first_channel*32767).astype(np.int16)
            plt.plot(first_channel_int16)
            plt.title("Microphone data")
            plt.show()
        if self.current_service:
            print("Current data length:",len(self.current))
            plt.plot(self.current_timestamp,self.current)
            plt.title("Current data")
            plt.show()

    ##### welding and current recording callbacks and functions #####
    def clean_weld_record(self):

        self.weld_timestamp=[]
        self.weld_voltage=[]
        self.weld_current=[]
        self.weld_feedrate=[]
        self.weld_energy=[]

    def clean_current_record(self):
        self.current=[]
        self.current_timestamp=[]

    def weld_cb(self, sub, value, ts):

        if self.start_weld_cb:
            # self.weld_timestamp.append(value.ts['microseconds'][0])
            self.weld_timestamp.append(time.perf_counter()+self.t_offset)
            self.weld_voltage.append(value.welding_voltage)
            self.weld_current.append(value.welding_current)
            self.weld_feedrate.append(value.wire_speed)
            self.weld_energy.append(value.welding_energy)

    def current_cb(self, sub, value, ts):
        if self.start_current_cb:
            # self.current_timestamp.append(ts.seconds+ts.nanoseconds*1e-9)
            self.current_timestamp.append(time.perf_counter()+self.t_offset)
            self.current.append(value)

    def save_weld_file(self,filedir):
        np.savetxt(filedir + 'welding.csv',
                   np.array([(np.array(self.weld_timestamp)), self.weld_voltage, self.weld_current, self.weld_feedrate, self.weld_energy]).T, delimiter=',',
                   header='timestamp,voltage,current,feedrate,energy', comments='')

    def save_current_file(self,filedir):
        np.savetxt(filedir + 'current.csv',
                   np.array([(np.array(self.current_timestamp)), self.current]).T, delimiter=',',
                   header='timestamp,current', comments='')

    ##### FLIR and Xiris camera callbacks and functions #####

    def clean_ir_record(self):
        self.ir_timestamp=[]
        self.ir_recording=[]

    def clean_ir_2_record(self):
        self.ir_2_timestamp=[]
        self.ir_2_recording=[]


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

                # Convert the packet to an image and set the global variable
                self.ir_recording.append(copy.deepcopy(display_mat))
                # self.ir_timestamp.append(rr_img.image_info.data_header.ts['seconds']+rr_img.image_info.data_header.ts['nanoseconds']*1e-9)
                self.ir_timestamp.append(time.perf_counter()+self.t_offset)

    def save_ir_file(self,filedir):
        with h5py.File(f"{filedir}ir_recording.h5", 'w') as file:
            file.create_dataset(
                'video_frames',
                data=np.array(self.ir_recording),
                chunks=self.ir_chunk_shape,
                compression=self.ir_compression_alg
            )
            file.create_dataset(
                'timestamps',
                data=self.ir_timestamp,
                compression=self.ir_compression_alg
            )

    def ir_2_cb(self,pipe_ep):
        # Loop to get the newest frame
        while (pipe_ep.Available > 0):
            # Receive the packet
            rr_img = pipe_ep.ReceivePacket()
            if self.start_ir_2_cb:
                # convert the packet to an image
                cv_img=self.img_2_util.image_to_array(rr_img)

                # save frame and timestamp
                self.ir_2_recording.append(cv_img)
                self.ir_2_timestamp.append(time.perf_counter()+self.t_offset)

    def save_ir_2_file(self,filedir):
        with h5py.File(f"{filedir}ir_2_recording.h5", 'w') as file:
            file.create_dataset(
                'video_frames',
                data=np.array(self.ir_2_recording),
                chunks=self.ir_2_chunk_shape,
                compression=self.ir_2_compression_alg
            )
            file.create_dataset(
                'timestamps',
                data=self.ir_2_timestamp,
                compression=self.ir_2_compression_alg
            )
    ##### FujiCam scanner callbacks and functions #####

    def fujicam_connect_failed_handler(self, s, client_id, url, err):
        print ("Client connect failed: " + str(client_id.NodeID) + " url: " + str(url) + " error: " + str(err))

    def clean_fujicam_record(self):
        self.fujicam_line_profiles=[]
        self.fujicam_timestamps=[]

    def fujicam_value_changed_handler(self, con, wire_packet_value, ts):
        if not self.start_fujicam_cb:
            return

        valid_indices=np.where(wire_packet_value.I_data>1)[0]
        valid_indices=np.intersect1d(valid_indices,np.where(np.abs(wire_packet_value.Z_data)>10)[0])
        line_profile=np.hstack((wire_packet_value.Y_data[valid_indices].reshape(-1,1),wire_packet_value.Z_data[valid_indices].reshape(-1,1)))
        self.fujicam_line_profiles.append(line_profile)
        self.fujicam_timestamps.append(time.perf_counter()+self.t_offset)

    def save_fujicam_file(self,filedir):
        with open(filedir+'line_scan.pickle','wb') as file:
            pickle.dump(self.fujicam_line_profiles,file)
        np.savetxt(filedir + "line_scan_stamps.csv",self.fujicam_timestamps,delimiter=',')
        # with h5py.File(f"{filedir}line_scan.h5", 'w') as file:
        #     file.create_dataset(
        #         'line_scans',
        #         data=np.array(self.fujicam_line_profiles),
        #         compression=self.fujicam_compression_alg
        #     )
        #     file.create_dataset(
        #         'timestamps',
        #         data=self.fujicam_timestamps,
        #         compression=self.fujicam_compression_alg
        #     )

    ##### Microphone callbacks and functions #####

    def clean_mic_record(self):

        self.audio_recording=[]

    def microphone_cb(self,pipe_ep):

        #Loop to get the newest frame
        while (pipe_ep.Available > 0):
            audio = pipe_ep.ReceivePacket().audio_data
            if self.start_mic_cb:
                #Receive the packet
                self.audio_recording.extend(audio)

    def save_mic_file(self,filedir):

        print("Mic length:",len(self.audio_recording))

        try:
            first_channel = np.concatenate(self.audio_recording)

            first_channel_int16=(first_channel*32767).astype(np.int16)
            with wave.open(filedir+'mic_recording.wav', 'wb') as wav_file:
                # Set the WAV file parameters
                wav_file.setnchannels(self.mic_channels)
                wav_file.setsampwidth(2)  # 2 bytes per sample (16-bit)
                wav_file.setframerate(self.mic_samplerate)

                # Write the audio data to the WAV file
                wav_file.writeframes(first_channel_int16.tobytes())
        except:
            print("Mic has no recording!!!")

    def save_data_streaming(self,recorded_dir,current_data,welding_data,audio_recording,robot_data,flir_logging,flir_ts, slice_num,section_num=0, xir_logging=None, xir_ts=None):
        ###MAKING DIR
        layer_data_dir=recorded_dir+'layer_'+str(slice_num)+'_'+str(section_num)+'/'
        Path(layer_data_dir).mkdir(exist_ok=True)

        ####AUDIO SAVING
        first_channel = np.concatenate(audio_recording)
        first_channel_int16=(first_channel*32767).astype(np.int16)
        with wave.open(layer_data_dir+'mic_recording.wav', 'wb') as wav_file:
            # Set the WAV file parameters
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)  # 2 bytes per sample (16-bit)
            wav_file.setframerate(44100)
            # Write the audio data to the WAV file
            wav_file.writeframes(first_channel_int16.tobytes())

        ####CURRENT SAVING
        np.savetxt(
            layer_data_dir + 'current.csv',
            current_data,
            delimiter=',',
            header='timestamp,current',
            comments=''
        )

        ####FRONIUS SAVING
        np.savetxt(
            layer_data_dir + 'welding.csv',
            welding_data,
            delimiter=',',
            header='timestamp,voltage,current,feedrate,energy',
            comments=''
        )


        ####ROBOT JOINT SAVING
        np.savetxt(layer_data_dir+'joint_recording.csv',robot_data,delimiter=',')

        ###FLIR SAVING
        flir_ts=np.array(flir_ts)
        with open(layer_data_dir+'ir_recording.pickle','wb') as file:
            pickle.dump(np.array(flir_logging),file)
        np.savetxt(layer_data_dir + "ir_stamps.csv",flir_ts,delimiter=',')

        ###XIR SAVING
        if xir_ts is not None:
            xir_ts=np.array(xir_ts)
            with open(layer_data_dir+'ir_2_recording.pickle','wb') as file:
                pickle.dump(np.array(xir_logging),file)
            np.savetxt(layer_data_dir + "ir_stamps_2.csv",xir_ts,delimiter=',')

        return
