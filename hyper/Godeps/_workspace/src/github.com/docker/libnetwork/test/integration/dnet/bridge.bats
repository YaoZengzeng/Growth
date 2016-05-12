# -*- mode: sh -*-
#!/usr/bin/env bats

load helpers

function test_single_network_connectivity() {
    local nw_name start end

    nw_name=${1}
    start=1
    end=${2}

    # Create containers and connect them to the network
    for i in `seq ${start} ${end}`;
    do
	dnet_cmd $(inst_id2port 1) container create container_${i}
	net_connect 1 container_${i} ${nw_name}
    done

    # Now test connectivity between all the containers using service names
    for i in `seq ${start} ${end}`;
    do
	runc $(dnet_container_name 1 bridge) $(get_sbox_id 1 container_${i}) \
	     "ping -c 1 www.google.com"
	for j in `seq ${start} ${end}`;
	do
	    if [ "$i" -eq "$j" ]; then
		continue
	    fi
	    runc $(dnet_container_name 1 bridge) $(get_sbox_id 1 container_${i}) \
		 "ping -c 1 container_${j}"
	done
    done

    if [ -n "$3" ]; then
	return
    fi

    # Teardown the container connections and the network
    for i in `seq ${start} ${end}`;
    do
	net_disconnect 1 container_${i} ${nw_name}
	dnet_cmd $(inst_id2port 1) container rm container_${i}
    done
}

@test "Test default bridge network" {
    skip_for_circleci

    echo $(docker ps)
    test_single_network_connectivity bridge 3
}


@test "Test default network dnet restart" {
    skip_for_circleci

    echo $(docker ps)

    for iter in `seq 1 2`;
    do
	test_single_network_connectivity bridge 3
	if [ "$iter" -eq 1 ]; then
	    docker restart dnet-1-bridge
	    wait_for_dnet $(inst_id2port 1) dnet-1-bridge
	fi
    done
}

@test "Test default network dnet ungraceful restart" {
    skip_for_circleci

    echo $(docker ps)

    for iter in `seq 1 2`;
    do
	if [ "$iter" -eq 1 ]; then
	    test_single_network_connectivity bridge 3 skip
	    docker restart dnet-1-bridge
	    wait_for_dnet $(inst_id2port 1) dnet-1-bridge
	else
	    test_single_network_connectivity bridge 3
	fi
    done
}

@test "Test bridge network" {
    skip_for_circleci

    echo $(docker ps)
    dnet_cmd $(inst_id2port 1) network create -d bridge singlehost
    test_single_network_connectivity singlehost 3
    dnet_cmd $(inst_id2port 1) network rm singlehost
}

@test "Test bridge network dnet restart" {
    skip_for_circleci

    echo $(docker ps)
    dnet_cmd $(inst_id2port 1) network create -d bridge singlehost

    for iter in `seq 1 2`;
    do
	test_single_network_connectivity singlehost 3
	if [ "$iter" -eq 1 ]; then
	    docker restart dnet-1-bridge
	    wait_for_dnet $(inst_id2port 1) dnet-1-bridge
	fi
    done

    dnet_cmd $(inst_id2port 1) network rm singlehost
}

@test "Test bridge network dnet ungraceful restart" {
    skip_for_circleci

    echo $(docker ps)
    dnet_cmd $(inst_id2port 1) network create -d bridge singlehost

    for iter in `seq 1 2`;
    do
	if [ "$iter" -eq 1 ]; then
	    test_single_network_connectivity singlehost 3 skip
	    docker restart dnet-1-bridge
	    wait_for_dnet $(inst_id2port 1) dnet-1-bridge
	else
	    test_single_network_connectivity singlehost 3
	fi
    done

    dnet_cmd $(inst_id2port 1) network rm singlehost
}

@test "Test multiple bridge networks" {
    skip_for_circleci

    echo $(docker ps)

    start=1
    end=3

    for i in `seq ${start} ${end}`;
    do
	dnet_cmd $(inst_id2port 1) container create container_${i}
	for j in `seq ${start} ${end}`;
	do
	    if [ "$i" -eq "$j" ]; then
		continue
	    fi

	    if [ "$i" -lt "$j" ]; then
		dnet_cmd $(inst_id2port 1) network create -d bridge sh${i}${j}
		nw=sh${i}${j}
	    else
		nw=sh${j}${i}
	    fi

	    osvc="svc${i}${j}"
	    dnet_cmd $(inst_id2port 1) service publish ${osvc}.${nw}
	    dnet_cmd $(inst_id2port 1) service attach container_${i} ${osvc}.${nw}
	done
    done

    for i in `seq ${start} ${end}`;
    do
	echo ${i1}
	for j in `seq ${start} ${end}`;
	do
	    echo ${j1}
	    if [ "$i" -eq "$j" ]; then
		continue
	    fi

	    osvc="svc${j}${i}"
	    echo "pinging ${osvc}"
	    dnet_cmd $(inst_id2port 1) service ls
	    runc $(dnet_container_name 1 bridge) $(get_sbox_id 1 container_${i}) "cat /etc/hosts"
	    runc $(dnet_container_name 1 bridge) $(get_sbox_id 1 container_${i}) "ping -c 1 ${osvc}"
	done
    done

    for i in `seq ${start} ${end}`;
    do
	for j in `seq ${start} ${end}`;
	do
	    if [ "$i" -eq "$j" ]; then
		continue
	    fi

	    if [ "$i" -lt "$j" ]; then
		nw=sh${i}${j}
	    else
		nw=sh${j}${i}
	    fi

	    osvc="svc${i}${j}"
	    dnet_cmd $(inst_id2port 1) service detach container_${i} ${osvc}.${nw}
	    dnet_cmd $(inst_id2port 1) service unpublish ${osvc}.${nw}

	done
	dnet_cmd $(inst_id2port 1) container rm container_${i}
    done

    for i in `seq ${start} ${end}`;
    do
	for j in `seq ${start} ${end}`;
	do
	    if [ "$i" -eq "$j" ]; then
		continue
	    fi

	    if [ "$i" -lt "$j" ]; then
		dnet_cmd $(inst_id2port 1) network rm sh${i}${j}
	    fi
	done
    done

}
