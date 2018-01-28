package cn.edu.scnu.dtindex.tools;


import cn.edu.scnu.dtindex.model.MBR;
import cn.edu.scnu.dtindex.model.RTree;
import cn.edu.scnu.dtindex.model.RTreeDiskSliceFile;
import cn.edu.scnu.dtindex.model.RTreeNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.io.File;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JPanel;


public class RTreeVirtualization extends JFrame {
	private static final long serialVersionUID = 1L;

	public RTreeVirtualization(String title) {
		super(title);
		this.setSize(600, 600);
		this.setLocationRelativeTo(null); // 设置窗口的位置屏幕中间
		this.setResizable(false); // 设置为不可改变大小
		this.setDefaultCloseOperation(EXIT_ON_CLOSE); // 设置关闭事件
		this.setBackground(Color.white);
		MyJPanel myJPanel = new MyJPanel(); // 创建一个MyJPanel实例对象myJPanel
		myJPanel.setBackground(Color.blue);// 设置myJPanel的背景颜色
		this.add(myJPanel);
		this.setVisible(true);
	}

	/* 内部继承JPanel类 */
	class MyJPanel extends JPanel {
		private static final long serialVersionUID = 1L;
		private double bili = 0;
		private double bili_x = 600 / (1577922954000.0/1000000);
		private double bili_y = 500 / (1577498200000.0/1000000)*10;

		protected void paintComponent(Graphics g) {
			Text key = new Text();
			RTreeDiskSliceFile value = new RTreeDiskSliceFile();
//File Path: hdfs://192.168.69.204:8020/timeData/1000w/DiskSliceFile/RTreeIndex/RTreeindex_partitioner_1_10300211.seq
			Configuration conf = new Configuration();
			try {
				SequenceFile.Reader reader =
						new SequenceFile.Reader(conf,
								SequenceFile.Reader.file(
										new Path("hdfs://192.168.69.204:8020/timeData/1000w/DiskSliceFile/RTreeIndex/RTreeindex_partitioner_1_10300211.seq")));
				reader.next(key, value);
				RTree index = value.getIndex();
				RTreeNode root = index.getRoot();
				CalcBili(root.getMbr());
				DrawMBR(g, root.getMbr(), 0);
				int i=4;
				for (RTreeNode node : root.getNodeList()) {
					for (RTreeNode level3 : node.getNodeList()) {
						//DrawMBR(g, level3.getMbr(), i--);
					}
					if (i==-1)i=4;
					DrawMBR(g, node.getMbr(), i--);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}


		}

		private void CalcBili(MBR Rootmbr) {
			long max = 0;
			max = max < Rootmbr.getBottomLeft().getStart() ? Rootmbr.getBottomLeft().getStart() : max;
			max = max < Rootmbr.getBottomLeft().getEnd() ? Rootmbr.getBottomLeft().getEnd() : max;
			max = max < Rootmbr.getTopRight().getStart() ? Rootmbr.getTopRight().getStart() : max;
			max = max < Rootmbr.getTopRight().getEnd() ? Rootmbr.getTopRight().getEnd() : max;
			max = max / 1000000;
			bili = 400 / (double) max;
		}

		private void DrawMBR(Graphics g, MBR mbr, int level) {
			Graphics2D g2 = (Graphics2D) g;
			g2.setStroke(new BasicStroke(2F));
			switch (level) {
				case 0:
					g2.setStroke(new BasicStroke(1F));
					g.setColor(Color.blue);
					break;
				case 1:
					g2.setStroke(new BasicStroke(1F));
					g.setColor(Color.ORANGE);
					break;

				case 2:
					g2.setStroke(new BasicStroke(1F));
					g.setColor(Color.green);
					break;
				case 3:
					g2.setStroke(new BasicStroke(1F));
					g.setColor(Color.red);
					break;
				case 4:
					g2.setStroke(new BasicStroke(1F));
					g.setColor(Color.BLACK);
					break;
				case 5:
					g2.setStroke(new BasicStroke(2F));
					g.setColor(Color.MAGENTA);
					break;
				case 6:
					g2.setStroke(new BasicStroke(2F));
					g.setColor(Color.yellow);
					break;
				default:
					break;
			}
			double bls = mbr.getBottomLeft().getStart()/1000000 * bili_x;
			double ble = mbr.getBottomLeft().getEnd()/1000000 * bili_y;
			//ble = 500 - ble;
			double trs = mbr.getTopRight().getStart()/1000000  * bili_x;
			double tre = mbr.getTopRight().getEnd()/1000000  * bili_y;
			//tre = 500 - tre;
			((Graphics2D) g).drawRect((int)bls,(int)ble,(int)(trs-bls),(int)(tre-ble));

			//g.drawLine((int) bls, (int) ble, (int) trs, (int) ble);
			//g.drawLine((int) bls, (int) tre, (int) trs, (int) tre);
			//g.drawLine((int) bls, (int) tre, (int) bls, (int) ble);
			//g.drawLine((int) trs, (int) tre, (int) trs, (int) ble);

		}


		private void paintLine(Graphics g, int level) throws Exception {

			Graphics2D g2 = (Graphics2D) g;
			g2.setStroke(new BasicStroke(2F));
			g.setColor(Color.blue);
            switch (level) {
                case 0:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.blue);
                    break;
                case 1:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.ORANGE);
                    break;

                case 2:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.green);
                    break;
                case 3:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.red);
                    break;
                case 4:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.BLACK);
                    break;
                case 5:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.MAGENTA);
                    break;
                case 6:
                    g2.setStroke(new BasicStroke(2F));
                    g.setColor(Color.yellow);
                    break;
                default:
                    break;
            }


			/*for (int i = 1; i < pos.length; i++) {
				//绘制pos[i-1]到pos[i]的一个路径
				if (pos[i - 1].start != pos[i].start && pos[i - 1].end != pos[i].end) {
					PointData corner = new PointData("", pos[i - 1].start, pos[i].end);
					g.drawLine(pos[i - 1].start * CONSTANTS.bili + CONSTANTS.fix,
							600 - pos[i - 1].end * CONSTANTS.bili + CONSTANTS.fix,
							corner.start * CONSTANTS.bili + CONSTANTS.fix,
							600 - corner.end * CONSTANTS.bili + CONSTANTS.fix);
					g.drawLine(corner.start * CONSTANTS.bili + CONSTANTS.fix,
							600 - corner.end * CONSTANTS.bili + CONSTANTS.fix,
							pos[i].start * CONSTANTS.bili + CONSTANTS.fix,
							600 - pos[i].end * CONSTANTS.bili + CONSTANTS.fix);
				} else {
					g.drawLine(pos[i - 1].start * CONSTANTS.bili + CONSTANTS.fix,
							600 - pos[i - 1].end * CONSTANTS.bili + CONSTANTS.fix,
							pos[i].start * CONSTANTS.bili + CONSTANTS.fix,
							600 - pos[i].end * CONSTANTS.bili + CONSTANTS.fix);
				}


			}*/


		}

		private void paintPoint(Graphics g) {
			/*try {
				PointData[] datas = PointData.ReadToData("/Users/think/Desktop/data.txt");
				Color c = g.getColor();
				g.setColor(Color.red);
				for (PointData data : datas) {
					g.fillOval(data.start * CONSTANTS.bili, 600 - (data.end * CONSTANTS.bili), CONSTANTS.ban_jin,
							CONSTANTS.ban_jin);
					g.drawString("(" + data.start + "," + data.end + ")", data.start * CONSTANTS.bili,
							600 - (data.end * CONSTANTS.bili) - CONSTANTS.ban_jin);
				}

				g.setColor(Color.GREEN);
				// g.drawLine(20 + 5, 600 - 60 + 5, 20 + 5, 600 - 30 + 5);
				g.setColor(c);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/

		}

	}

	public static void main(String[] args) {
		new RTreeVirtualization("");
	}
}