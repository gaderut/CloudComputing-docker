### Analytics Component Ports

#### PORTS

Define 6 dedicated ports for analytics please avoid these six ports for other components.

- 12340,12341,12342,12343,12344,12345

#### APIs

1.

```
/get_name
```

Get workflow name and client name and workflow specification.


2. 

```
/analytics_init
```

Initilzes analytics component call with workflow start request.

3. 

```
/put_result
```

Send result to analytics call with prediction request.

4. 

```
/get_result
```

Get all prediction result call by client.
 
