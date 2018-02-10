cd /home/gmrtest/analytics
Process1=$(pgrep -f -x "python AnalyticsToolv1_1.py")
if [ ! -z "$Process1" -a "$Process1" != " " ]; then
        echo "Process Running"
else
        echo "Process is not running"
	python AnalyticsToolv1_1.py
fi
