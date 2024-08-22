from general_robotics_toolbox import *
import numpy as np
from scipy.interpolate import interp1d
import copy
from lambda_calc import *

def spiralize(traj1,traj2,reversed=False):
	###interpolate traj1 to traj2 with spiral printing
	###interp traj2 to be of same length

	traj2_interp=interp1d(np.linspace(0,1,num=len(traj2)),traj2,axis=0, bounds_error=False, fill_value="extrapolate")(np.linspace(0,1,num=len(traj1)))
	if not reversed:
		weight=np.linspace(1,0.5,num=len(traj1))
	else:
		weight=np.linspace(0.5,1,num=len(traj1))

	traj_new = weight[:, np.newaxis] * traj1 + (1 - weight[:, np.newaxis]) * traj2_interp
	
	return traj_new

def warp_traj(rob1_js,rob2_js,positioner_js,rob1_js_x,rob2_js_x,positioner_js_x,reversed=False):
	if positioner_js_x.shape==(2,) and rob1_js_x.shape==(6,):
		return rob1_js,rob2_js,positioner_js
	traj_length_x_half=int(len(rob1_js_x)/2)
	traj_length_half=int(len(rob1_js)/2)
	if reversed:
		rob1_js[:traj_length_half]=spiralize(rob1_js[:traj_length_half],rob1_js_x[:traj_length_x_half],reversed)
		rob2_js[:traj_length_half]=spiralize(rob2_js[:traj_length_half],rob2_js_x[:traj_length_x_half],reversed)
		positioner_js[:traj_length_half]=spiralize(positioner_js[:traj_length_half],positioner_js_x[:traj_length_x_half],reversed)
	else:
		rob1_js[traj_length_half:]=spiralize(rob1_js[traj_length_half:],rob1_js_x[traj_length_x_half:],reversed)
		rob2_js[traj_length_half:]=spiralize(rob2_js[traj_length_half:],rob2_js_x[traj_length_x_half:],reversed)
		positioner_js[traj_length_half:]=spiralize(positioner_js[traj_length_half:],positioner_js_x[traj_length_x_half:],reversed)
	return rob1_js,rob2_js,positioner_js

def warp_traj2(rob1_js,positioner_js,rob1_js_x,positioner_js_x,reversed=False):
	#same functionality, but without robot2
	if positioner_js_x.shape==(2,) and rob1_js_x.shape==(6,):
		return rob1_js,positioner_js
	traj_length_x_half=int(len(rob1_js_x)/2)
	traj_length_half=int(len(rob1_js)/2)
	if reversed:
		rob1_js[:traj_length_half]=spiralize(rob1_js[:traj_length_half],rob1_js_x[:traj_length_x_half],reversed)
		positioner_js[:traj_length_half]=spiralize(positioner_js[:traj_length_half],positioner_js_x[:traj_length_x_half],reversed)
	else:
		rob1_js[traj_length_half:]=spiralize(rob1_js[traj_length_half:],rob1_js_x[traj_length_x_half:],reversed)
		positioner_js[traj_length_half:]=spiralize(positioner_js[traj_length_half:],positioner_js_x[traj_length_x_half:],reversed)
	return rob1_js,positioner_js


def weld_spiral(robot,positioner,data_dir,v,feedrate,slice_increment,num_layers,slice_start,slice_end,job_offset,waypoint_distance=5,flipped=False,q_prev=np.zeros(2)):
	###generate welding primitives for spiral welding by connecting slices
	q1_all=[]
	q2_all=[]
	v1_all=[]
	v2_all=[]
	cond_all=[]
	primitives=[]
	arcon_set=False
	###PRELOAD ALL SLICES TO SAVE INPROCESS TIME
	rob1_js_all_slices=[]
	positioner_js_all_slices=[]
	for i in range(0,slice_end):
		if not flipped:
			rob1_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/MA2010_js'+str(i)+'_0.csv',delimiter=','))
			positioner_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/D500B_js'+str(i)+'_0.csv',delimiter=','))
		else:
			###spiral rotation direction
			rob1_js_all_slices.append(np.flip(np.loadtxt(data_dir+'curve_sliced_js/MA2010_js'+str(i)+'_0.csv',delimiter=','),axis=0))
			positioner_js_all_slices.append(np.flip(np.loadtxt(data_dir+'curve_sliced_js/D500B_js'+str(i)+'_0.csv',delimiter=','),axis=0))

	print("LAYERS PRELOAD FINISHED")

	slice_num=slice_start
	layer_counts=0
	while layer_counts<num_layers:

		####################DETERMINE CURVE ORDER##############################################
		x=0
		rob1_js=copy.deepcopy(rob1_js_all_slices[slice_num])
		positioner_js=copy.deepcopy(positioner_js_all_slices[slice_num])
		curve_sliced_relative=np.loadtxt(data_dir+'curve_sliced_relative/slice'+str(slice_num)+'_'+str(x)+'.csv',delimiter=',')
		if positioner_js.shape==(2,) and rob1_js.shape==(6,):
			continue
		
		###TRJAECTORY WARPING
		if slice_num>0:
			rob1_js_prev=copy.deepcopy(rob1_js_all_slices[slice_num-slice_increment])
			positioner_js_prev=copy.deepcopy(positioner_js_all_slices[slice_num-slice_increment])
			rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_prev,positioner_js_prev,reversed=True)
		if slice_num<slice_end-slice_increment:
			rob1_js_next=copy.deepcopy(rob1_js_all_slices[slice_num+slice_increment])
			positioner_js_next=copy.deepcopy(positioner_js_all_slices[slice_num+slice_increment])
			rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_next,positioner_js_next,reversed=False)
				
		
			
		lam_relative=calc_lam_cs(curve_sliced_relative)
		lam1=calc_lam_js(rob1_js,robot)
		lam2=calc_lam_js(positioner_js,positioner)
		num_points_layer=max(2,int(lam_relative[-1]/waypoint_distance))
		breakpoints=np.linspace(0,len(rob1_js)-1,num=num_points_layer).astype(int)
		s1_all,s2_all=calc_individual_speed(v,lam1,lam2,lam_relative,breakpoints)
		# s1_all=[0.1]*len(s1_all)
		###find closest %2pi
		num2p=np.round((q_prev-positioner_js[0])/(2*np.pi))
		positioner_js+=num2p*2*np.pi
		###no need for acron/off when spiral, positioner not moving at all
		if not arcon_set:
			arcon_set=True
			q1_all.append(rob1_js[breakpoints[0]])
			q2_all.append(positioner_js[breakpoints[0]])
			v1_all.append(1)
			v2_all.append(1)
			cond_all.append(0)
			primitives.append('movej')
		

		q1_all.extend(rob1_js[breakpoints[1:]].tolist())
		q2_all.extend(positioner_js[breakpoints[1:]].tolist())
		v1_all.extend([1]*len(s1_all))
		cond_all.extend([int(feedrate/10)+job_offset]*(num_points_layer-1))
		primitives.extend(['movel']*(num_points_layer-1))

		for j in range(1,len(breakpoints)):
			positioner_w=v/np.linalg.norm(curve_sliced_relative[breakpoints[j]][:2])
			v2_all.append(min(100,100*positioner_w/positioner.joint_vel_limit[1]))
		
		q_prev=copy.deepcopy(positioner_js[-1])
		layer_counts+=1
		slice_num+=slice_increment

	return primitives,q1_all,q2_all,v1_all,v2_all,cond_all



def weld_spiral_streaming(SS,data_dir,v,slice_increment,num_layers,slice_start,slice_end,point_distance=0.04,flipped=False,q_positioner_prev=np.zeros(2)):
	###generate streaming points for spiral welding by connecting slices

	q1_cmd_all=[]
	positioner_cmd_all=[]
	###PRELOAD ALL SLICES TO SAVE INPROCESS TIME
	rob1_js_all_slices=[]
	positioner_js_all_slices=[]
	for i in range(0,slice_end):
		if not flipped:
			rob1_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/MA2010_js'+str(i)+'_0.csv',delimiter=','))
			positioner_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/D500B_js'+str(i)+'_0.csv',delimiter=','))
		else:
			###spiral rotation direction
			rob1_js_all_slices.append(np.flip(np.loadtxt(data_dir+'curve_sliced_js/MA2010_js'+str(i)+'_0.csv',delimiter=','),axis=0))
			positioner_js_all_slices.append(np.flip(np.loadtxt(data_dir+'curve_sliced_js/D500B_js'+str(i)+'_0.csv',delimiter=','),axis=0))

	print("LAYERS PRELOAD FINISHED")

	slice_num=slice_start
	layer_counts=0
	while layer_counts<num_layers:

		####################DETERMINE CURVE ORDER##############################################
		x=0
		rob1_js=copy.deepcopy(rob1_js_all_slices[slice_num])
		positioner_js=copy.deepcopy(positioner_js_all_slices[slice_num])
		curve_sliced_relative=np.loadtxt(data_dir+'curve_sliced_relative/slice'+str(slice_num)+'_'+str(x)+'.csv',delimiter=',')
		if positioner_js.shape==(2,) and rob1_js.shape==(6,):
			continue
		
		###TRJAECTORY WARPING
		if slice_num>0:
			rob1_js_prev=copy.deepcopy(rob1_js_all_slices[slice_num-slice_increment])
			positioner_js_prev=copy.deepcopy(positioner_js_all_slices[slice_num-slice_increment])
			rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_prev,positioner_js_prev,reversed=True)
		if slice_num<slice_end-slice_increment:
			rob1_js_next=copy.deepcopy(rob1_js_all_slices[slice_num+slice_increment])
			positioner_js_next=copy.deepcopy(positioner_js_all_slices[slice_num+slice_increment])
			rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_next,positioner_js_next,reversed=False)
				
		
			
		lam_relative=calc_lam_cs(curve_sliced_relative)
		lam_relative_dense=np.linspace(0,lam_relative[-1],num=int(lam_relative[-1]/point_distance))
		rob1_js_dense=interp1d(lam_relative,rob1_js,kind='cubic',axis=0)(lam_relative_dense)
		positioner_js_dense=interp1d(lam_relative,positioner_js,kind='cubic',axis=0)(lam_relative_dense)
		breakpoints=SS.get_breakpoints(lam_relative_dense,v)

		###find closest %2pi
		num2p=np.round((q_positioner_prev-positioner_js_dense[0])/(2*np.pi))
		positioner_js_dense+=num2p*2*np.pi
		
		###formulate streaming joint angles
		q1_cmd_all.extend(rob1_js_dense[breakpoints])
		positioner_cmd_all.extend(positioner_js_dense[breakpoints])
		
		q_positioner_prev=copy.deepcopy(positioner_js_dense[-1])

		layer_counts+=1
		slice_num+=slice_increment

	return np.array(q1_cmd_all),np.array(positioner_cmd_all)


def weld_bf_streaming(SS,data_dir,v,slice_increment,num_layers,slice_start,slice_end,point_distance=0.04,q_positioner_prev=np.zeros(2),layer_name=''):
	###generate streaming points for back&forth welding by connecting slices

	q1_cmd_all=[]
	positioner_cmd_all=[]
	###PRELOAD ALL SLICES TO SAVE INPROCESS TIME
	rob1_js_all_slices=[]
	positioner_js_all_slices=[]
	for i in range(0,slice_end):
		rob1_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/MA2010_'+layer_name+'js'+str(i)+'_0.csv',delimiter=','))
		positioner_js_all_slices.append(np.loadtxt(data_dir+'curve_sliced_js/D500B_'+layer_name+'js'+str(i)+'_0.csv',delimiter=','))

	print("LAYERS PRELOAD FINISHED")

	slice_num=slice_start
	layer_counts=0
	while layer_counts<num_layers:

		####################DETERMINE CURVE ORDER##############################################
		x=0
		rob1_js=copy.deepcopy(rob1_js_all_slices[slice_num])
		positioner_js=copy.deepcopy(positioner_js_all_slices[slice_num])
		curve_sliced_relative=np.loadtxt(data_dir+'curve_sliced_relative/'+layer_name+'slice%i_%i.csv'%(slice_num,x),delimiter=',')
		if positioner_js.shape==(2,) and rob1_js.shape==(6,):
			continue
		###TRJAECTORY WARPING
		if layer_counts%2==0:
			if slice_num>0:
				rob1_js_prev=copy.deepcopy(rob1_js_all_slices[slice_num-slice_increment])
				positioner_js_prev=copy.deepcopy(positioner_js_all_slices[slice_num-slice_increment])
				rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_prev,positioner_js_prev,reversed=True)
			if slice_num<slice_end-slice_increment:
				rob1_js_next=copy.deepcopy(rob1_js_all_slices[slice_num+slice_increment])
				positioner_js_next=copy.deepcopy(positioner_js_all_slices[slice_num+slice_increment])
				rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_next,positioner_js_next,reversed=False)
		else:
			if slice_num>0:
				rob1_js_prev=copy.deepcopy(rob1_js_all_slices[slice_num-slice_increment])
				positioner_js_prev=copy.deepcopy(positioner_js_all_slices[slice_num-slice_increment])
				rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_prev,positioner_js_prev,reversed=False)
			if slice_num<slice_end-slice_increment:
				rob1_js_next=copy.deepcopy(rob1_js_all_slices[slice_num+slice_increment])
				positioner_js_next=copy.deepcopy(positioner_js_all_slices[slice_num+slice_increment])
				rob1_js,positioner_js=warp_traj2(rob1_js,positioner_js,rob1_js_next,positioner_js_next,reversed=True)
		
		lam_relative=calc_lam_cs(curve_sliced_relative)
		lam_relative_dense=np.linspace(0,lam_relative[-1],num=int(lam_relative[-1]/point_distance))
		rob1_js_dense=interp1d(lam_relative,rob1_js,kind='cubic',axis=0)(lam_relative_dense)
		positioner_js_dense=interp1d(lam_relative,positioner_js,kind='cubic',axis=0)(lam_relative_dense)
		breakpoints=SS.get_breakpoints(lam_relative_dense,v)

		###find closest %2pi
		num2p=np.round((q_positioner_prev-positioner_js_dense[0])/(2*np.pi))
		positioner_js_dense+=num2p*2*np.pi
		
		###formulate streaming joint angles
		if layer_counts%2==0:
			q1_cmd_all.extend(rob1_js_dense[breakpoints])
			positioner_cmd_all.extend(positioner_js_dense[breakpoints])
		else:
			q1_cmd_all.extend(np.flip(rob1_js_dense[breakpoints],axis=0))
			positioner_cmd_all.extend(np.flip(positioner_js_dense[breakpoints],axis=0))
		
		q_positioner_prev=copy.deepcopy(positioner_js_dense[-1])

		layer_counts+=1
		slice_num+=slice_increment

	return np.array(q1_cmd_all),np.array(positioner_cmd_all)

