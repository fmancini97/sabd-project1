#!/bin/bash
echo Configuring cockroach
/cockroach/cockroach.sh init --host cockroach-1 --insecure
/cockroach/cockroach.sh sql --host cockroach-1 --insecure < /cockroach-config/project1_database.sql
echo Cockroach configured