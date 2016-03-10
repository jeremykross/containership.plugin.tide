# tide

## About

### Build Status
[![Build Status](https://drone.containership.io/api/badges/containership/containership.plugin.tide/status.svg)](https://drone.containership.io/containership/containership.plugin.tide)

### Description
A cron-like job scheduler for ContainerShip.

### Author
ContainerShip Developers - developers@containership.io

## Usage

### Install
`cs plugin add tide`

### Update
`cs plugin update tide`

### Remove
`cs plugin remove tide`

### List Jobs
`cs tide list-jobs`

### Create Job
`cs tide create-job example --image myorg/mycron:1.2.2 --cpus 0.1 --memory 128 --env-var KEY=value --instances 1 --schedule "0 0 * * *"`

### Edit Job
`cs tide edit-job example --image myorg/mycron:1.2.3`

### Show Job
`cs tide show-job example`

### Remove Job
`cs tide remove-job example`

## Contributing
Pull requests and issues are encouraged!
