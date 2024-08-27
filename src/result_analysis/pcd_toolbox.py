import open3d as o3d
from sklearn.decomposition import PCA
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from copy import deepcopy
import colorsys
from robotics_utils import *
from scipy.spatial import KDTree


def global_alignment(pc1,pc2):
    ###align pc1 to pc2 with PCA
    pca1 = PCA()
    pca1.fit(np.array(pc1))
    R1 = pca1.components_.T
    pca2 = PCA()
    pca2.fit(np.array(pc2))
    R2 = pca2.components_.T

    #make sure the rotation matrix is right-handed
    if np.cross(R1[:,0],R1[:,1])@R1[:,2]<0:
        R1[:,2]=-R1[:,2]   
    if np.cross(R2[:,0],R2[:,1])@R2[:,2]<0:
        R2[:,2]=-R2[:,2]

    R=R2@R1.T
    p=np.mean(np.array(pc2),axis=0)-np.mean(pc1@R.T,axis=0)

    pc1_transformed=pc1@R.T+p
    kdtree = KDTree(pc2)
    distances, indices = kdtree.query(pc1_transformed)
    rmse = np.sqrt(np.mean(np.square(distances)))

    min_rmse=rmse
    R_best=R
    p_best=p
    ###rotate about x,y,z axis to find minimum rmse
    for R1 in [R1@Rx(np.pi),R1@Ry(np.pi),R1@Rz(np.pi)]:
        R=R2@R1.T
        p=np.mean(np.array(pc2),axis=0)-np.mean(pc1@R.T,axis=0)
        pc1_transformed=pc1@R.T+p
        kdtree = KDTree(pc2)
        distances, indices = kdtree.query(pc1_transformed)
        rmse = np.sqrt(np.mean(np.square(distances)))
        if rmse<min_rmse:
            min_rmse=rmse
            R_best=R
            p_best=p
    
    return R_best,p_best


def colormap(all_h):

    all_h=(1-all_h)*270
    all_color=[]
    for h in all_h:
        all_color.append(colorsys.hsv_to_rgb(h,0.7,0.9))
    return np.array(all_color)

def display_inlier_outlier(cloud, ind):
    
    if type(cloud) is o3d.cpu.pybind.t.geometry.PointCloud:
        cloud_show = cloud.to_legacy()
    else:
        cloud_show=cloud

    points_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=20,origin=[0,0,0])
    inlier_cloud = cloud_show.select_by_index(ind)
    outlier_cloud = cloud_show.select_by_index(ind, invert=True)

    outlier_cloud.paint_uniform_color([1, 0, 0])
    inlier_cloud.paint_uniform_color([0, 0.8, 0])
    show_pcd_list=[inlier_cloud, outlier_cloud, points_frame]
    o3d.visualization.draw_geometries(show_pcd_list,width=960,height=540)

def visualize_pcd(show_pcd_list,point_show_normal=False):

    show_pcd_list_legacy=[]
    for obj in show_pcd_list:
        if type(obj) is o3d.cpu.pybind.t.geometry.PointCloud or type(obj) is o3d.cpu.pybind.t.geometry.TriangleMesh:
            show_pcd_list_legacy.append(obj.to_legacy())
        else:
            show_pcd_list_legacy.append(obj)

    points_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=20,origin=[0,0,0])
    show_pcd_list_legacy.append(points_frame)
    o3d.visualization.draw_geometries(show_pcd_list_legacy,width=960,height=540,point_show_normal=point_show_normal)

def visualize_frames(all_R,all_p,size=20):

    all_points_frame = []
    for i in range(len(all_R)):
        points_frame = o3d.geometry.TriangleMesh.create_coordinate_frame(size=size)
        points_frame = points_frame.rotate(all_R[i],center=[0,0,0])
        points_frame = points_frame.translate(all_p[i])
        
        all_points_frame.append(points_frame)
    o3d.visualization.draw_geometries(all_points_frame,width=960,height=540)


def calc_error_projected(target_points,collapsed_points):
    num_points=10
    error=[]
    for p in collapsed_points:
        indices=np.argsort(np.linalg.norm(target_points-p,axis=1))[:num_points]
        normal, centroid=fit_plane(target_points[indices])
        error.append(np.linalg.norm(vector_to_plane(p, centroid, normal)))     ###vector from surface to left
    return np.array(error)

def separate_by_z(scanned_points,target_points):
    num_points=10
    left_indices=[]
    right_indices=[]

    for i in range(len(scanned_points)):
        indices=np.argsort(np.linalg.norm(target_points-scanned_points[i],axis=1))[:num_points]
        normal, centroid=fit_plane(target_points[indices])
        v=vector_to_plane(scanned_points[i], centroid, normal)
        if np.dot(v,np.array([0,0,20]))<0:
            left_indices.append(i)
        else:
            right_indices.append(i)

    return left_indices,right_indices

def separate_by_y(scanned_points,target_points):
    num_points=10
    left_indices=[]
    right_indices=[]

    for i in range(len(scanned_points)):
        indices=np.argsort(np.linalg.norm(target_points-scanned_points[i],axis=1))[:num_points]
        normal, centroid=fit_plane(target_points[indices])
        v=vector_to_plane(scanned_points[i], centroid, normal)
        if np.dot(v,np.array([0,10,0]))<0:
            left_indices.append(i)
        else:
            right_indices.append(i)

    return left_indices,right_indices


def collapse(left_pc,right_pc,target_points):
    num_points=10
    collapsed_surface=[]
    width=[]
    for i in range(len(left_pc)):
        indices=np.argsort(np.linalg.norm(target_points-left_pc[i],axis=1))[:num_points]
        normal, centroid=fit_plane(target_points[indices])
        v1=-vector_to_plane(left_pc[i], centroid, normal)     ###vector from surface to left
        indices_right=np.argsort(np.linalg.norm(right_pc-(left_pc[i]-2*v1),axis=1))[:num_points]
        normal_right, centroid_right=fit_plane(right_pc[indices_right])
        v2=vector_to_plane(left_pc[i]-v1, centroid_right, normal_right)
        width.append([np.linalg.norm(v1),np.linalg.norm(v2)])
        collapsed_surface.append(left_pc[i]-v1+(v1+v2)/2)
    
    return np.sum(width,axis=1), collapsed_surface