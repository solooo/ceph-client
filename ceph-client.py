#!/usr/bin/env python
# -*- coding:utf-8 -*-
# python 2.6
import rados
import MySQLdb as mydb


def conn_ceph():
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    return cluster

def search_objId(ioctx):
    if ioctx is None:
        print '未连接到ceph'
        return

    conn = mydb.connect(host='192.168.1.48', 
                        port=3306, 
                        user='root', 
                        passwd='Sonar@1234', 
                        db='video-storage',
                        charset='utf8')
    cursor = conn.cursor(cursorclass=mydb.cursors.DictCursor)
    try:
        cursor.execute('select * from file_info')
        rs = cursor.fetchall()
        for r in rs:
            try:
                ioctx.remove_object(str(r['oid']))
                print 'remove oid: '+ r['oid']
            except Exception as e:
                print e
            try:
                ioctx.remove_object(str(r['frame_index_file_oid']))
                print 'remove frame_index: ' + r['frame_index_file_oid']
                cursor.execute('delete from file_info where oid="{0}"'.format(r['oid']))
            except Exception as e1:
                print e1
    except Exception as ex:
        print ex
    finally:
        cursor.close()
        conn.commit()
        conn.close()

cluster = conn_ceph()
pools = cluster.list_pools()
ioctx = cluster.open_ioctx('pool1')
search_objId(ioctx)
ioctx.close()
cluster.shutdown()

